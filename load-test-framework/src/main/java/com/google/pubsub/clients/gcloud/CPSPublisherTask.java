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
package com.google.pubsub.clients.gcloud;

import com.beust.jcommander.JCommander;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.AbstractPublisher;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.common.LoadtestTask;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Runs a task that publishes messages to a Cloud Pub/Sub topic.
 */
public class CPSPublisherTask extends AbstractPublisher {

  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final ByteString payload;
  private final Publisher publisher;
  private final String orderingKeysBase;


  CPSPublisherTask(StartRequest request, MetricsHandler metricsHandler, int workerCount) {
    super(request, metricsHandler, workerCount);
    log.warn("constructing CPS publisher");
    this.payload = getPayload();
    this.orderingKeysBase = UUID.randomUUID().toString();
    try {

      Publisher.Builder builder =
          Publisher.newBuilder(ProjectTopicName.of(request.getProject(), request.getTopic()))
              .setBatchingSettings(
                  BatchingSettings.newBuilder()
                      .setElementCountThreshold((long) request.getPublisherOptions().getBatchSize())
                      .setRequestByteThreshold(9500000L)
                      .setDelayThreshold(
                          Duration.ofMillis(
                              Durations.toMillis(request.getPublisherOptions().getBatchDuration())))
                      .setIsEnabled(true)
                      .build());
      if (this.enableOrdering) {
        builder.setEnableMessageOrdering(true);
      }
      if (StringUtils.isNotBlank(request.getPubsubOptions().getApiRootUrl())) {
        URL pubsubUrl = new URL(request.getPubsubOptions().getApiRootUrl());
        log.error("Subscribing to pubsub at: {}:{}", pubsubUrl.getHost(), pubsubUrl.getDefaultPort());
        InstantiatingGrpcChannelProvider loadtestProvider =
            InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(pubsubUrl.getHost() + ":" + pubsubUrl.getDefaultPort()).build();
        builder.setChannelProvider(loadtestProvider);
      }
      this.publisher = builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ListenableFuture<Void> publish(
      int clientId, int sequenceNumber, long publishTimestampMillis) {
    SettableFuture<Void> done = SettableFuture.create();
    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder()
        .setData(this.payload)
        .putAttributes("sendTime", Long.toString(publishTimestampMillis))
        .putAttributes("clientId", Integer.toString(clientId))
        .putAttributes("sequenceNumber", Integer.toString(sequenceNumber));
    if (enableOrdering) {
      final String key =
          this.orderingKeysBase + "_" + clientId + "_" + (sequenceNumber % numOrderingKeys);
      sequenceNumber = sequenceNumber / numOrderingKeys;
      messageBuilder.setOrderingKey(key);
      messageBuilder.putAttributes("sequenceNumber", Integer.toString(sequenceNumber));
    }
    ApiFutures.addCallback(
        publisher.publish(messageBuilder.build()), new ApiFutureCallback<String>() {
          @Override
          public void onSuccess(String messageId) {
            if (enableOrdering) {
              verifierWriter.write(messageBuilder.getOrderingKey(), Integer.parseInt(messageBuilder.getAttributesMap().get("sequenceNumber")));
            }
            done.set(null);
          }

          @Override
          public void onFailure(Throwable t) {
            done.setException(t);
          }
        }, MoreExecutors.directExecutor());
    return done;
  }


  @Override
  public void cleanup() {
    try {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Failed to shutdown " + CPSPublisherTask.class.getSimpleName() + "properly");
      throw new RuntimeException(e);
    }
    super.cleanup();
  }

  public static class CPSPublisherFactory implements Factory {

    @Override
    public LoadtestTask newTask(StartRequest request, MetricsHandler handler, int numWorkers) {
      return new CPSPublisherTask(request, handler, numWorkers);
    }

  }

  public static void main(String[] args) throws Exception {
    JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
    new JCommander(options, args);
    new JavaLoadtestWorker(options, new CPSPublisherFactory());
  }
}
