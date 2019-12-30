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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorSpec;
import org.apache.druid.indexing.seekablestream.SeekableStreamSamplerSpec;
import org.apache.druid.segment.indexing.TuningConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class PulsarSamplerSpec extends SeekableStreamSamplerSpec
{

  @JsonCreator
  public PulsarSamplerSpec(
      @JsonProperty("spec") final PulsarSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    super(ingestionSpec, samplerConfig, inputSourceSampler);
  }

  @Override
  protected PulsarRecordSupplierTask createRecordSupplier()
  {
    final Map<String, Object> props = new HashMap<>(((PulsarSupervisorIOConfig) ioConfig).getConsumerProperties());

    String serviceUrl = (String) props.get(PulsarSupervisorIOConfig.SERVICE_URL);

    int maxRowsInMemory = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;

    if (tuningConfig != null) {
      maxRowsInMemory = tuningConfig.convertToTaskTuningConfig().getMaxRowsInMemory();
    }

    return new PulsarRecordSupplierTask(serviceUrl, "druid-pulsar-indexing-sampler", maxRowsInMemory);
  }
}
