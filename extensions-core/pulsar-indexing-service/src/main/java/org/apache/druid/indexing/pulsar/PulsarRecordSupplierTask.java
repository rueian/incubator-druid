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

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.common.naming.TopicName;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PulsarRecordSupplierTask extends PulsarRecordSupplierBase implements ReaderListener<byte[]>
{
  private final String taskId;
  private final int maxRecordsInSinglePoll;

  private final BlockingQueue<Message<byte[]>> received;

  PulsarRecordSupplierTask(
      String serviceUrl,
      String taskId,
      int maxRecordsInSinglePoll
  )
  {
    super(serviceUrl);
    this.taskId = taskId;
    this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;
    this.received = new ArrayBlockingQueue<>(this.maxRecordsInSinglePoll);
  }

  @Override
  protected CompletableFuture<Reader<byte[]>> buildConsumer(PulsarClient client, String topic)
  {
    return client.newReader()
                 .readerName(taskId)
                 .topic(topic)
                 .readerListener(this)
                 .startMessageId(MessageId.earliest)
                 .createAsync();
  }

  private StreamPartition<Integer> getStreamPartitionFromMessage(Message<byte[]> msg)
  {
    TopicName topic = TopicName.get(msg.getTopicName());
    return new StreamPartition<>(topic.getPartitionedTopicName(), topic.getPartitionIndex());
  }

  @NotNull
  @Override
  public List<OrderedPartitionableRecord<Integer, Long>> poll(long timeout)
  {
    try {
      List<OrderedPartitionableRecord<Integer, Long>> records = new ArrayList<>();


      Message<byte[]> item = received.poll(timeout, TimeUnit.MILLISECONDS);
      if (item == null) {
        return records;
      }

      int numberOfRecords = 0;

      while (item != null) {
        StreamPartition<Integer> sp = getStreamPartitionFromMessage(item);
        Long offset = PulsarSequenceNumber.of(item.getMessageId()).get();

        records.add(new OrderedPartitionableRecord<>(
            sp.getStream(),
            sp.getPartitionId(),
            offset,
            ImmutableList.of(item.getValue())
        ));

        setPosition(sp, offset);

        if (++numberOfRecords >= maxRecordsInSinglePoll) {
          break;
        }

        // Check if we have an item already available
        item = received.poll(0, TimeUnit.MILLISECONDS);
      }

      return records;
    }
    catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void received(Reader<byte[]> reader, Message<byte[]> message)
  {
    try {
      this.received.put(message);
    }
    catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }
}
