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

package com.google.pubsub.clients.common;

import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.clients.flow_control.FlowController;
import com.google.pubsub.clients.flow_control.OutstandingCountFlowController;
import com.google.pubsub.clients.flow_control.RateLimiterFlowController;
import com.google.pubsub.flic.common.LoadtestProto;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPublisher extends PooledWorkerTask {
  private static final Logger log = LoggerFactory.getLogger(AbstractPublisher.class);
  private static final LogEveryN logEveryN = new LogEveryN(log, 500);

  private final ByteString payload;
  private final double perThreadRateUpperBound;
  // Start at 100 kB/s/thread
  private static final double STARTING_PER_THREAD_BYTES_PER_SEC = Math.pow(10, 5);
  private final boolean flowControlLimitExists;
  private final double startingPerThreadRate;
  protected final int numOrderingKeys;
  protected final boolean enableOrdering;
  protected final VerifierWriter verifierWriter;



  public AbstractPublisher(
      LoadtestProto.StartRequest request, MetricsHandler handler, int workerCount) {
    super(request, handler, workerCount);
    this.payload = createMessage(request.getPublisherOptions().getMessageSize());
    this.numOrderingKeys = request.getNumOrderingKeysPerThread();
    this.enableOrdering = this.numOrderingKeys > 0 ? true : false;
    this.verifierWriter = new VerifierWriter("order_input.csv");
    if (request.getPublisherOptions().getRate() <= 0) {
      this.perThreadRateUpperBound = Double.MAX_VALUE;
      this.flowControlLimitExists = false;
      this.startingPerThreadRate = STARTING_PER_THREAD_BYTES_PER_SEC / payload.size();
      log.info("Per thread rate to start: " + startingPerThreadRate);
    } else {
      this.perThreadRateUpperBound = request.getPublisherOptions().getRate() / workerCount;
      this.flowControlLimitExists = true;
      this.startingPerThreadRate = 0;
      log.info("Per thread rate upper bound: " + perThreadRateUpperBound);
    }
  }

  protected ByteString getPayload() {
    return payload;
  }

  /** Creates a string message of a certain size. */
  private ByteString createMessage(int msgSize) {
    byte[] payloadArray = new byte[msgSize];
    Arrays.fill(payloadArray, (byte) 'A');
    return ByteString.copyFrom(payloadArray);
  }

  @Override
  protected void cleanup() {
    this.verifierWriter.shutdown();
  }

  @Override
  protected void startAction() {
    int id = (new Random()).nextInt();
    int sequence_number = 0;
    FlowController flowController;
    if (flowControlLimitExists) {
      flowController = new RateLimiterFlowController(perThreadRateUpperBound);
    } else {
      flowController = new OutstandingCountFlowController(startingPerThreadRate);
    }
    while (!isShutdown.get()) {
      int permits = flowController.requestStart();
      for (int i = 0; i < permits; i++) {
        long publishTimestampMillis = System.currentTimeMillis();
        LoadtestProto.MessageIdentifier identifier =
            LoadtestProto.MessageIdentifier.newBuilder()
                .setSequenceNumber(sequence_number)
                .setPublisherClientId(id)
                .build();
        Futures.addCallback(
            publish(id, sequence_number++, publishTimestampMillis),
            new FutureCallback<Void>() {
              public void onSuccess(Void result) {
                metricsHandler.add(
                    identifier,
                    Duration.ofMillis(System.currentTimeMillis() - publishTimestampMillis));
                flowController.informFinished(true);
              }

              public void onFailure(Throwable t) {
                metricsHandler.addFailure();
                flowController.informFinished(false);
                logEveryN.error("Publisher error: " + t);
              }
            },
            MoreExecutors.directExecutor());
      }
    }
  }

  protected abstract ListenableFuture<Void> publish(
      int clientId, int sequenceNumber, long publishTimestampMillis);
}
