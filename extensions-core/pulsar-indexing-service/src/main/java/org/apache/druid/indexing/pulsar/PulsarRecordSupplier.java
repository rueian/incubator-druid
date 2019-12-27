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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.PulsarKafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class PulsarRecordSupplier implements RecordSupplier<Integer, Long>
{
  private final PulsarKafkaConsumer<byte[], byte[]> consumer;
  private boolean closed;

  public PulsarRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper sortingMapper
  )
  {
    this(getPulsarKafkaConsumer(sortingMapper, consumerProperties));
  }

  @VisibleForTesting
  public PulsarRecordSupplier(
      PulsarKafkaConsumer<byte[], byte[]> consumer
  )
  {
    this.consumer = consumer;
  }

  public static void addConsumerPropertiesFromConfig(
      Properties properties,
      ObjectMapper configMapper,
      Map<String, Object> consumerProperties
  )
  {
    // Extract passwords before SSL connection to Kafka
    for (Map.Entry<String, Object> entry : consumerProperties.entrySet()) {
      String propertyKey = entry.getKey();
      if (propertyKey.equals(PulsarSupervisorIOConfig.TRUST_STORE_PASSWORD_KEY)
          || propertyKey.equals(PulsarSupervisorIOConfig.KEY_STORE_PASSWORD_KEY)
          || propertyKey.equals(PulsarSupervisorIOConfig.KEY_PASSWORD_KEY)) {
        PasswordProvider configPasswordProvider = configMapper.convertValue(
            entry.getValue(),
            PasswordProvider.class
        );
        properties.setProperty(propertyKey, configPasswordProvider.getPassword());
      } else {
        properties.setProperty(propertyKey, String.valueOf(entry.getValue()));
      }
    }
  }

  private static PulsarKafkaConsumer<byte[], byte[]> getPulsarKafkaConsumer(
      ObjectMapper sortingMapper,
      Map<String, Object> consumerProperties
  )
  {
    final Map<String, Object> consumerConfigs = PulsarConsumerConfigs.getConsumerProperties();
    final Properties props = new Properties();
    addConsumerPropertiesFromConfig(props, sortingMapper, consumerProperties);
    props.putAll(consumerConfigs);
    props.put("key.deserializer", StringDeserializer.class.getTypeName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getTypeName());

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(PulsarRecordSupplier.class.getClassLoader());

      return new PulsarKafkaConsumer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private static <T> T wrapExceptions(Callable<T> callable)
  {
    try {
      return callable.call();
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  private static void wrapExceptions(Runnable runnable)
  {
    wrapExceptions(() -> {
      runnable.run();
      return null;
    });
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    wrapExceptions(() -> consumer.subscribe(streamPartitions
                                                .stream()
                                                .map(StreamPartition::getStream)
                                                .collect(Collectors.toSet())));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    wrapExceptions(() -> consumer.seek(
        new TopicPartition(partition.getStream(), partition.getPartitionId()),
        sequenceNumber
    ));
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> partitions)
  {
    wrapExceptions(() -> consumer.seekToBeginning(partitions
                                                      .stream()
                                                      .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                                                      .collect(Collectors.toList())));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> partitions)
  {
    wrapExceptions(() -> consumer.seekToEnd(partitions
                                                .stream()
                                                .map(e -> new TopicPartition(e.getStream(), e.getPartitionId()))
                                                .collect(Collectors.toList())));
  }

  @Override
  public Set<StreamPartition<Integer>> getAssignment()
  {
    return wrapExceptions(() -> consumer.subscription()
                                        .stream()
                                        .map(e -> new StreamPartition<>(e, 0))
                                        .collect(Collectors.toSet()));
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<Integer, Long>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<Integer, Long>> polledRecords = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(timeout)) {
      polledRecords.add(new OrderedPartitionableRecord<>(
          record.topic(),
          record.partition(),
          record.offset(),
          record.value() == null ? null : ImmutableList.of(record.value())
      ));
    }
    return polledRecords;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    Long currPos = getPosition(partition);
    seekToLatest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    Long currPos = getPosition(partition);
    seekToEarliest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getPosition(StreamPartition<Integer> partition)
  {
    return wrapExceptions(() -> consumer.position(new TopicPartition(
        partition.getStream(),
        partition.getPartitionId()
    )));
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    Set<Integer> ids = new HashSet<>();
    ids.add(0);
    return ids;
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    consumer.close();
  }
}
