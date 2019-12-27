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

import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PulsarRecordSupplierSupervisor extends PulsarRecordSupplierBase
{
  private static final String TASK_ID = "druid-pulsar-indexing-service-supervisor";
  private final ConcurrentHashMap<StreamPartition<Integer>, Long> positions = new ConcurrentHashMap<>();

  public PulsarRecordSupplierSupervisor(String serviceUrl)
  {
    super(serviceUrl);
  }

  @Override
  protected CompletableFuture<Reader<byte[]>> buildConsumer(PulsarClient client, String topic)
  {
    return client.newReader()
                 .readerName(TASK_ID)
                 .topic(topic)
                 .startMessageId(MessageId.latest)
                 .startMessageIdInclusive()
                 .createAsync();
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> positions.put(p, PulsarSequenceNumber.LATEST_OFFSET));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    positions.put(partition, sequenceNumber);
  }

  private CompletableFuture<Long> findEdge(StreamPartition<Integer> partition, Boolean useEarliestOffset)
  {
    String topic = TopicName.get(partition.getStream()).getPartition(partition.getPartitionId()).toString();
    return client.newReader()
                 .readerName(TASK_ID)
                 .topic(topic)
                 .startMessageId(useEarliestOffset ? MessageId.earliest : MessageId.latest)
                 .startMessageIdInclusive()
                 .createAsync()
                 .thenApply(reader -> {
                   try {
                     Message<byte[]> msg = reader.readNext(0, TimeUnit.MILLISECONDS);
                     if (msg != null) {
                       return PulsarSequenceNumber.of(msg.getMessageId()).get();
                     }
                   }
                   catch (PulsarClientException e) {
                     throw new StreamException(e);
                   }
                   finally {
                     reader.closeAsync();
                   }

                   return useEarliestOffset ? PulsarSequenceNumber.EARLIEST_OFFSET : PulsarSequenceNumber.LATEST_OFFSET;
                 });
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
  {
    CompletableFuture.allOf(streamPartitions.stream()
                                            .map(p ->
                                                     findEdge(p, true)
                                                         .thenApply(offset -> positions.put(p, offset))
                                            )
                                            .toArray(CompletableFuture[]::new));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
  {
    CompletableFuture.allOf(streamPartitions.stream()
                                            .map(p ->
                                                     findEdge(p, false)
                                                         .thenApply(offset -> positions.put(p, offset))
                                            )
                                            .toArray(CompletableFuture[]::new));
  }

  @Override
  public Long getPosition(StreamPartition<Integer> partition)
  {
    Long position = positions.get(partition);
    if (position == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }
    return position;
  }

  @NotNull
  @Override
  public List<OrderedPartitionableRecord<Integer, Long>> poll(long timeout)
  {
    return new ArrayList<>();
  }
}
