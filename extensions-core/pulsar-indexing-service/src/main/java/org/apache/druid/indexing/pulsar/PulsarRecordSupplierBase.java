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
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class PulsarRecordSupplierBase implements RecordSupplier<Integer, Long>
{
  protected final ConcurrentHashMap<StreamPartition<Integer>, Container> readers = new ConcurrentHashMap<>();
  protected final PulsarClient client;
  protected PulsarClientException previousSeekFailure;

  PulsarRecordSupplierBase(String serviceUrl)
  {
    try {
      ClientBuilder clientBuilder = PulsarClient.builder();
      // Since this client instance is going to be used just for the consumers, we can enable Nagle to group
      // all the acknowledgments sent to broker within a short time frame
      clientBuilder.enableTcpNoDelay(false);
      client = clientBuilder.serviceUrl(serviceUrl).build();
    }
    catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract CompletableFuture<Reader<byte[]>> buildConsumer(PulsarClient client, String topic);

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    List<CompletableFuture<Reader<byte[]>>> futures = new ArrayList<>();

    try {
      for (StreamPartition<Integer> partition : streamPartitions) {
        if (readers.containsKey(partition)) {
          continue;
        }

        String topic = TopicName.get(partition.getStream())
                                .getPartition(partition.getPartitionId())
                                .toString();

        futures.add(buildConsumer(client, topic).thenApplyAsync(reader -> {
          if (readers.containsKey(partition)) {
            reader.closeAsync();
          } else {
            readers.put(partition, new Container(reader, PulsarSequenceNumber.EARLIEST_OFFSET));
          }
          return reader;
        }));
      }

      futures.forEach(CompletableFuture::join);
    }
    catch (Exception e) {
      futures.forEach(f -> {
        try {
          f.get().closeAsync();
        }
        catch (Exception ignored) {
          // ignore
        }
      });
      throw new StreamException(e);
    }
  }

  public PulsarClientException getPreviousSeekFailure()
  {
    return previousSeekFailure;
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber) throws InterruptedException
  {
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }

    try {
      reader.consumer.seek(PulsarSequenceNumber.of(sequenceNumber).getMessageId());
      setPosition(partition, sequenceNumber);
      previousSeekFailure = null;
    }
    catch (PulsarClientException e) {
      previousSeekFailure = e;
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.EARLIEST_OFFSET);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.LATEST_OFFSET);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public Collection<StreamPartition<Integer>> getAssignment()
  {
    return this.readers.keySet();
  }

  @NotNull
  @Override
  public abstract List<OrderedPartitionableRecord<Integer, Long>> poll(long timeout);

  @Nullable
  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.LATEST_OFFSET;
  }

  @Nullable
  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.EARLIEST_OFFSET;
  }

  @Override
  public Long getPosition(StreamPartition<Integer> partition)
  {
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }
    return reader.offset;
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    try {
      return client.getPartitionsForTopic(stream).get().stream()
                   .map(TopicName::get)
                   .map(TopicName::getPartitionIndex)
                   .collect(Collectors.toSet());
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void close()
  {
    readers.forEach((k, r) -> r.consumer.closeAsync());
    client.closeAsync();
  }

  protected void setPosition(StreamPartition<Integer> partition, Long offset)
  {
    Container reader = this.readers.get(partition);
    if (reader != null) {
      reader.offset = offset;
    }
  }

  public static class Container
  {
    public Reader<byte[]> consumer;
    public Long offset;

    public Container(Reader<byte[]> consumer, Long offset)
    {
      this.consumer = consumer;
      this.offset = offset;
    }
  }
}
