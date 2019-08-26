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

import static com.google.pubsub.flic.common.LoadtestProto.SubscriberOptions.SubscriberProperties.IN_ORDER;

import com.beust.jcommander.JCommander;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Timestamp;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.common.LoadtestTask;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.VerifierWriter;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.SubscriberOptions.SubscriberProperties;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a task that consumes messages from a Cloud Pub/Sub subscription. */
public class CPSSubscriberTask implements LoadtestTask, MessageReceiver {

  private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);
  private static final long BYTES_PER_WORKER = 100000000; // 100MB per worker outstanding
  private static final long MAX_RECEIPT_INTERVAL_MILLIS = 30 * 1000;
  private static final long MAX_SHUTDOWN_WAIT_MILLIS = 2 * 60 * 1000;
  // Maximum expected decrease from publisher throughput (if exceeded, force shutdown).
  private static final double ALLOWABLE_SUBSCRIBER_THTOUGHPUT_LAG = 0.9;
  private final MetricsHandler metricsHandler;
  private final Subscriber subscriber;
  private final String subscriberId;
  private Map<String, Integer> orderingKeyLatestReceived;
  private Map<String, Integer> orderingKeyToReset;
  private final Set<SubscriberProperties> invariants;
  private final VerifierWriter verifierWriter;
  private final boolean enableOrdering;
  private final StartRequest startRequest;

  private CPSSubscriberTask(StartRequest request, MetricsHandler metricsHandler, int workerCount) {
    this.startRequest = request;
    this.subscriberId = UUID.randomUUID().toString();
    this.metricsHandler = metricsHandler;
    this.orderingKeyLatestReceived = new ConcurrentHashMap<>(
        10 * startRequest.getNumOrderingKeysPerThread() * workerCount);
    this.verifierWriter = new VerifierWriter("order_output_" + this.subscriberId + ".csv");
    log.error("Required properties: {}",
        request.getSubscriberOptions().getRequiredPropertiesList());
    this.invariants = request.getSubscriberOptions().getRequiredPropertiesList().stream().collect(
        Collectors.toSet());
    log.error("registered subscriber invariants: {}", this.invariants);
    this.enableOrdering = this.invariants.contains(IN_ORDER);
    ProjectSubscriptionName subscription =
        ProjectSubscriptionName.of(
            request.getProject(), request.getPubsubOptions().getSubscription());
    try {
       Subscriber.Builder builder = Subscriber.newBuilder(subscription, this)
              .setParallelPullCount(workerCount)
              .setFlowControlSettings(
                  FlowControlSettings.newBuilder()
                      .setMaxOutstandingElementCount(Long.MAX_VALUE)
                      .setMaxOutstandingRequestBytes(BYTES_PER_WORKER * workerCount)
                      .build());
      if (StringUtils.isNotBlank(request.getPubsubOptions().getApiRootUrl())) {
        URL pubsubUrl = new URL(request.getPubsubOptions().getApiRootUrl());
        log.error("Subscribing to pubsub at: {}:{}", pubsubUrl.getHost(), pubsubUrl.getDefaultPort());
        InstantiatingGrpcChannelProvider loadtestProvider =
            InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(pubsubUrl.getHost() + ":" + pubsubUrl.getDefaultPort())
                .setMaxInboundMessageSize(20 * 1024 * 1024)
                .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(5))
                .setPoolSize(workerCount)
                .build();
        builder.setChannelProvider(loadtestProvider);
      }
      this.subscriber = builder.build();
      log.info("Started subscriber task subscriberId={}", this.subscriberId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {
    String clientId = message.getAttributesMap().get("clientId");
    Integer sequenceNumber = Integer.parseInt(message.getAttributesMap().get("sequenceNumber"));
    String orderingKey = message.getOrderingKey();
    Instant time = Instant.now();
    if (this.invariants.contains(IN_ORDER)) {
      this.verifierWriter.write(orderingKey, sequenceNumber);
      int expectedSequenceNumber =
          this.orderingKeyLatestReceived.getOrDefault(orderingKey, Integer.MAX_VALUE - 1) + 1;
      if (sequenceNumber > expectedSequenceNumber) {
        log.error(
            "Received message out-of-order: ordering_key={}, last_sequence={}, current_sequence={}",
            orderingKey, expectedSequenceNumber - 1, sequenceNumber);
        this.metricsHandler.addInvariantFailed(IN_ORDER);
      }
      this.orderingKeyLatestReceived.put(orderingKey, sequenceNumber);
    }
    this.metricsHandler.add(
        LoadtestProto.MessageIdentifier.newBuilder()
            .setPublisherClientId(Integer.parseInt(clientId))
            .setSequenceNumber(sequenceNumber)
            .setSubscriberClientId(this.subscriberId)
            .setOrderingKey(orderingKey)
            .setReceivedTime(Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build())
            .build(),
        Duration.ofMillis(System.currentTimeMillis() - Long
            .parseLong(message.getAttributesMap().get("sendTime"))));

    consumer.ack();
  }

  @Override
  public void start() {
    try {
      subscriber.startAsync().awaitRunning();
    } catch (Exception e) {
      log.error("Fatal error from subscriber.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    LocalDateTime stopRequestReceived = LocalDateTime.now();
    long maxWaitForReceipt = MAX_SHUTDOWN_WAIT_MILLIS;
    if (startRequest.getPublisherOptions().getRate() > 0) {
      Double waitTimeMillis =
          (startRequest.getPublisherOptions().getRate() / (1.0
              - ALLOWABLE_SUBSCRIBER_THTOUGHPUT_LAG)) *
              startRequest.getTestDuration().getSeconds() * 1000.0;
      maxWaitForReceipt = waitTimeMillis.longValue();
    }
    while (
        Duration.between(metricsHandler.getLastUpdated(), LocalDateTime.now()).toMillis()
            < MAX_RECEIPT_INTERVAL_MILLIS
            && Duration.between(stopRequestReceived, LocalDateTime.now()).toMillis()
            < maxWaitForReceipt) {
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        log.error("Error while waiting for subscriber to complete receiving messages", e);
        break;
      }
    }
    log.error("Stopping subscriber at {}s after receving stop request",
        ChronoUnit.SECONDS.between(stopRequestReceived, LocalDateTime.now()));
    subscriber.stopAsync().awaitTerminated();
    this.verifierWriter.shutdown();
  }

  public static class CPSSubscriberFactory implements Factory {

    @Override
    public LoadtestTask newTask(StartRequest request, MetricsHandler handler, int workerCount) {
      return new CPSSubscriberTask(request, handler, workerCount);
    }
  }

  public static void main(String[] args) throws Exception {
    JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
    new JCommander(options, args);
    new JavaLoadtestWorker(options, new CPSSubscriberFactory());
  }
}
