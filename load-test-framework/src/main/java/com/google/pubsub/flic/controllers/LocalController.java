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
package com.google.pubsub.flic.controllers;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.flic.controllers.resource_controllers.ComputeResourceController;
import com.google.pubsub.flic.controllers.resource_controllers.LocalComputeResourceController;
import com.google.pubsub.flic.controllers.resource_controllers.PubsubResourceController;
import com.google.pubsub.flic.controllers.resource_controllers.ResourceController;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;

/** This is a subclass of {@link Controller} that controls local load tests. */
public class LocalController extends ControllerBase {

  /** Instantiates the load test on Google Compute Engine. */
  private LocalController(
      ScheduledExecutorService executor,
      List<ResourceController> controllers,
      List<ComputeResourceController> computeControllers) {
    super(executor, controllers, computeControllers);
  }

  /** Returns a LocalController using default application credentials. */
  public static LocalController newLocalController(
      String projectName, Map<ClientParams, Integer> clients, ScheduledExecutorService executor) {
    try {
      HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = new JacksonFactory();
      GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
      if (credential.createScopedRequired()) {
        credential =
            credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
      }
      ArrayList<ResourceController> controllers = new ArrayList<>();
      ArrayList<ComputeResourceController> computeControllers = new ArrayList<>();
      String apiRoot = "";
      boolean enableOrdering = false;
      if (!clients.isEmpty()) {
        for (Map.Entry<ClientParams, Integer> paramsToCount: clients.entrySet()) {
          ClientParams params = paramsToCount.getKey();
          Integer numWorkers = paramsToCount.getValue();
          if (!StringUtils.equals(params.getTestParameters().apiRootUrl(), Pubsub.DEFAULT_ROOT_URL)) {
            apiRoot = params.getTestParameters().apiRootUrl();
          }
          if (params.getTestParameters().numOrderingKeysPerPublisherThread() > 0) {
            enableOrdering = true;
          }
          ComputeResourceController computeController =
              new LocalComputeResourceController(params, numWorkers, executor);
          controllers.add(computeController);
          computeControllers.add(computeController);
        }
      }

      Pubsub pubsub =
          new Pubsub.Builder(transport, jsonFactory, credential)
              .setApplicationName("Cloud Pub/Sub Loadtest Framework")
              .setRootUrl(StringUtils.defaultIfBlank(apiRoot, Pubsub.DEFAULT_ROOT_URL))
              .build();

      controllers.add(
          new PubsubResourceController(
              projectName, Client.TOPIC, ImmutableList.of(Client.SUBSCRIPTION), executor,
              enableOrdering, pubsub));
      return new LocalController(executor, controllers, computeControllers);
    } catch (Throwable t) {
      log.error("Unable to initialize GCE: ", t);
      return null;
    }
  }
}
