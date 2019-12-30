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

package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.utils.RandomIdUtils;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.pulsar.PulsarDataSourceMetadata;
import org.apache.druid.indexing.pulsar.PulsarIndexTask;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskClientFactory;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskIOConfig;
import org.apache.druid.indexing.pulsar.PulsarIndexTaskTuningConfig;
import org.apache.druid.indexing.pulsar.PulsarRecordSupplierSupervisor;
import org.apache.druid.indexing.pulsar.PulsarSequenceNumber;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the PulsarIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link PulsarSupervisorSpec} which includes the Pulsar topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Pulsar topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Pulsar offsets.
 */
public class PulsarSupervisor extends SeekableStreamSupervisor<Integer, Long>
{
  public static final TypeReference<TreeMap<Integer, Map<Integer, Long>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(PulsarSupervisor.class);
  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final PulsarSupervisorSpec spec;
  private volatile Map<Integer, Long> latestSequenceFromStream;

  public PulsarSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final PulsarIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final PulsarSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        StringUtils.format("PulsarSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        mapper,
        spec,
        rowIngestionMetersFactory,
        false
    );

    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
  }


  @Override
  protected RecordSupplier<Integer, Long> setupRecordSupplier()
  {
    String serviceUrl = (String) getIoConfig().getConsumerProperties()
                                              .get(PulsarSupervisorIOConfig.SERVICE_URL);
    return new PulsarRecordSupplierSupervisor(serviceUrl);
  }

  @Override
  protected void scheduleReporting(ScheduledExecutorService reportingExec)
  {
    PulsarSupervisorIOConfig ioConfig = spec.getIoConfig();
    PulsarSupervisorTuningConfig tuningConfig = spec.getTuningConfig();
    reportingExec.scheduleAtFixedRate(
        updateCurrentAndLatestOffsets(),
        ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
        Math.max(
            tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
        ),
        TimeUnit.MILLISECONDS
    );

    reportingExec.scheduleAtFixedRate(
        emitLag(),
        ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
        monitorSchedulerConfig.getEmitterPeriod().getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  @Override
  protected int getTaskGroupIdForPartition(Integer partitionId)
  {
    return partitionId % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof PulsarDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof PulsarIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<Integer, Long> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  )
  {
    PulsarSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<Integer, Long> partitionLag = getLagPerPartition(getHighestCurrentOffsets());
    return new PulsarSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getTopic(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        includeOffsets ? latestSequenceFromStream : null,
        includeOffsets ? partitionLag : null,
        includeOffsets ? partitionLag.values().stream().mapToLong(x -> Math.max(x, 0)).sum() : null,
        includeOffsets ? sequenceLastUpdated : null,
        spec.isSuspended(),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getSupervisorState(),
        stateManager.getExceptionEvents()
    );
  }


  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
      int groupId,
      Map<Integer, Long> startPartitions,
      Map<Integer, Long> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<Integer> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  )
  {
    PulsarSupervisorIOConfig pulsarIoConfig = (PulsarSupervisorIOConfig) ioConfig;
    return new PulsarIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(pulsarIoConfig.getTopic(), startPartitions, Collections.emptySet()),
        new SeekableStreamEndSequenceNumbers<>(pulsarIoConfig.getTopic(), endPartitions),
        pulsarIoConfig.getConsumerProperties(),
        pulsarIoConfig.getPollTimeout(),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat(
            spec.getDataSchema().getParser() == null ? null : spec.getDataSchema().getParser().getParseSpec()
        )
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<Integer, Long>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<Integer, Long>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);

    List<SeekableStreamIndexTask<Integer, Long>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(baseSequenceName, RandomIdUtils.getRandomId());
      taskList.add(new PulsarIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (PulsarIndexTaskTuningConfig) taskTuningConfig,
          (PulsarIndexTaskIOConfig) taskIoConfig,
          context,
          null,
          null,
          rowIngestionMetersFactory,
          sortingMapper,
          null
      ));
    }
    return taskList;
  }


  @Override
  // suppress use of CollectionUtils.mapValues() since the valueMapper function is dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  protected Map<Integer, Long> getLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return currentOffsets
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> latestSequenceFromStream != null
                     && latestSequenceFromStream.get(e.getKey()) != null
                     && e.getValue() != null
                     ? latestSequenceFromStream.get(e.getKey()) - e.getValue()
                     : Integer.MIN_VALUE
            )
        );
  }

  @Override
  protected PulsarDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<Integer, Long> map)
  {
    return new PulsarDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return PulsarSequenceNumber.of(seq);
  }

  private Runnable emitLag()
  {
    return () -> {
      try {
        Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();
        String dataSource = spec.getDataSchema().getDataSource();

        if (latestSequenceFromStream == null) {
          throw new ISE("Latest offsets from Pulsar have not been fetched");
        }

        if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
          log.warn(
              "Lag metric: Pulsar partitions %s do not match task partitions %s",
              latestSequenceFromStream.keySet(),
              highestCurrentOffsets.keySet()
          );
        }

        Map<Integer, Long> partitionLags = getLagPerPartition(highestCurrentOffsets);
        long maxLag = 0, totalLag = 0, avgLag;
        for (long lag : partitionLags.values()) {
          if (lag > maxLag) {
            maxLag = lag;
          }
          totalLag += lag;
        }
        avgLag = partitionLags.size() == 0 ? 0 : totalLag / partitionLags.size();

        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/pulsar/lag", totalLag)
        );
        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/pulsar/maxLag", maxLag)
        );
        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/pulsar/avgLag", avgLag)
        );
      }
      catch (Exception e) {
        log.warn(e, "Unable to compute Pulsar lag");
      }
    };
  }

  @Override
  protected Long getNotSetMarker()
  {
    return NOT_SET;
  }

  @Override
  protected Long getEndOfPartitionMarker()
  {
    return END_OF_PARTITION;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum)
  {
    return false;
  }

  @Override
  protected boolean isShardExpirationMarker(Long seqNum)
  {
    return false;
  }

  @Override
  protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
  {
    return false;
  }

  @Override
  protected void updateLatestSequenceFromStream(
      RecordSupplier<Integer, Long> recordSupplier,
      Set<StreamPartition<Integer>> partitions
  )
  {
    latestSequenceFromStream = partitions.stream()
                                         .collect(Collectors.toMap(
                                             StreamPartition::getPartitionId,
                                             recordSupplier::getPosition
                                         ));
  }

  @Override
  protected String baseTaskName()
  {
    return "index_pulsar";
  }

  @Override
  @VisibleForTesting
  public PulsarSupervisorIOConfig getIoConfig()
  {
    return spec.getIoConfig();
  }

  @VisibleForTesting
  public PulsarSupervisorTuningConfig getTuningConfig()
  {
    return spec.getTuningConfig();
  }
}
