/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SequenceMetadata;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Pulsar indexing task runner supporting incremental segments publishing
 */
public class PulsarIndexTaskRunner extends SeekableStreamIndexTaskRunner<Integer, Long>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarIndexTaskRunner.class);
  private final PulsarIndexTask task;

  PulsarIndexTaskRunner(
      PulsarIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      LockGranularity lockGranularityToUse
  )
  {
    super(
        task,
        parser,
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory,
        appenderatorsManager,
        lockGranularityToUse
    );
    this.task = task;
  }

  @Override
  protected Long getNextStartOffset(@NotNull Long sequenceNumber)
  {
    return sequenceNumber;
  }

  @Nonnull
  @Override
  protected List<OrderedPartitionableRecord<Integer, Long>> getRecords(
      RecordSupplier<Integer, Long> recordSupplier,
      TaskToolbox toolbox
  ) throws IOException, InterruptedException
  {
    // Handles PulsarClientException, which is thrown if the seeked-to
    // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
    // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
    PulsarClientException previousSeekFailure = ((PulsarRecordSupplierTask) recordSupplier).getPreviousSeekFailure();
    if (previousSeekFailure == null) {
      return recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }

    log.warn("PulsarClientException with message [%s]", previousSeekFailure.getMessage());
    possiblyResetOffsetsOrWait(recordSupplier, toolbox);

    return new ArrayList<>();
  }

  @Override
  protected SeekableStreamEndSequenceNumbers<Integer, Long> deserializePartitionsFromMetadata(
      ObjectMapper mapper,
      Object object
  )
  {
    return mapper.convertValue(object, mapper.getTypeFactory().constructParametrizedType(
        SeekableStreamEndSequenceNumbers.class,
        SeekableStreamEndSequenceNumbers.class,
        Integer.class,
        Long.class
    ));
  }

  private void possiblyResetOffsetsOrWait(
      RecordSupplier<Integer, Long> recordSupplier,
      TaskToolbox taskToolbox
  ) throws InterruptedException, IOException
  {
    final Map<StreamPartition<Integer>, Long> resetPartitions = new HashMap<>();
    if (task.getTuningConfig().isResetOffsetAutomatically()) {
      for (StreamPartition<Integer> p : recordSupplier.getAssignment()) {
        final Long leastAvailableOffset = recordSupplier.getEarliestSequenceNumber(p);
        recordSupplier.seek(p, leastAvailableOffset);
        resetPartitions.put(p, leastAvailableOffset);
      }
      sendResetRequestAndWait(resetPartitions, taskToolbox);
    }
  }

  @Override
  protected SeekableStreamDataSourceMetadata<Integer, Long> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<Integer, Long> partitions
  )
  {
    return new PulsarDataSourceMetadata(partitions);
  }

  @Override
  protected OrderedSequenceNumber<Long> createSequenceNumber(Long sequenceNumber)
  {
    return PulsarSequenceNumber.of(sequenceNumber);
  }

  @Override
  protected void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<Integer, Long> recordSupplier,
      Set<StreamPartition<Integer>> assignment
  )
  {
    // do nothing
  }

  @Override
  protected boolean isEndOffsetExclusive()
  {
    return true;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum)
  {
    return false;
  }

  @Override
  public TypeReference<List<SequenceMetadata<Integer, Long>>> getSequenceMetadataTypeReference()
  {
    return new TypeReference<List<SequenceMetadata<Integer, Long>>>()
    {
    };
  }

  @Nullable
  @Override
  protected TreeMap<Integer, Map<Integer, Long>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException
  {
    if (checkpointsString != null) {
      log.debug("Got checkpoints from task context[%s].", checkpointsString);
      return toolbox.getJsonMapper().readValue(
          checkpointsString,
          new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
          {
          }
      );
    } else {
      return null;
    }
  }
}

