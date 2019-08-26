/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.controllers.test_parameters;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.Optional;

import static com.google.api.services.pubsub.Pubsub.DEFAULT_ROOT_URL;

@AutoValue
public abstract class TestParameters {
  public abstract int messageSize();

  public abstract int publishBatchSize();

  public abstract Optional<Integer> publishRatePerSec();

  public abstract Duration loadtestDuration();

  public abstract Duration burnInDuration();

  public abstract Duration publishBatchDuration();

  public abstract int numCoresPerWorker();

  public abstract int numPublisherWorkers();

  public abstract int numSubscriberWorkers();

  public abstract int cpuScaling();

  public abstract int numOrderingKeysPerPublisherThread();

  public abstract String apiRootUrl();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_TestParameters.Builder()
        .setApiRootUrl(DEFAULT_ROOT_URL)
        .setMessageSize(1000)
        .setPublishBatchDuration(Durations.fromMillis(50))
        .setPublishBatchSize(1000)
        .setBurnInDuration(Durations.fromSeconds(2*60))
        .setLoadtestDuration(Durations.fromSeconds(2*60))
        .setNumPublisherWorkers(1)
        .setNumOrderingKeysPerPublisherThread(0)
        .setNumSubscriberWorkers(1)
        .setCpuScaling(5);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setMessageSize(int messageSize);

    abstract Builder setPublishBatchSize(int publishBatchSize);

    abstract Builder setPublishRatePerSec(Optional<Integer> publishRatePerSec);

    abstract Builder setLoadtestDuration(Duration loadtestDuration);

    abstract Builder setBurnInDuration(Duration burnInDuration);

    abstract Builder setPublishBatchDuration(Duration publishBatchDuration);

    abstract Builder setNumCoresPerWorker(int numCoresPerWorker);

    abstract Builder setNumPublisherWorkers(int publisherWorkers);

    abstract Builder setNumSubscriberWorkers(int subscriberWorkers);

    abstract Builder setCpuScaling(int subscriberCpuScaling);

    abstract Builder setNumOrderingKeysPerPublisherThread(int numOrderingKeysPerPublisher);

    abstract Builder setApiRootUrl(String apiRootUrl);

    abstract TestParameters build();
  }
}
