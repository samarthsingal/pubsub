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

package com.google.pubsub.flic.common;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that no message loss has occurred.
 */
public class MessageTracker {

  private static final Logger log = LoggerFactory.getLogger(MessageTracker.class);

  // Map from publisher id to a set of sequence numbers.
  private Map<Long, Set<Long>> sentMessages = new HashMap<>();
  // Map from publisher id to a map from sequence numbers to received count.
  private Map<Long, Map<Long, Integer>> receivedMessages = new HashMap<>();
  // Map from subscriber id to received messages with ordering keys
  private Map<String, List<ReceivedOrderedMessage>> subscriberToOrderingKeys = new HashMap<>();

  private class ReceivedOrderedMessage {

    String orderingKey;
    Timestamp receivedTime;
    long sequeunceNumber;

    public ReceivedOrderedMessage(String orderingKey, Timestamp receivedTime, long sequeunceNumber) {
      this.orderingKey = orderingKey;
      this.receivedTime = receivedTime;
      this.sequeunceNumber = sequeunceNumber;
    }

    public ReceivedOrderedMessage(String orderingKey) {
      this.orderingKey = orderingKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReceivedOrderedMessage that = (ReceivedOrderedMessage) o;
      return orderingKey.equals(that.orderingKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(orderingKey);
    }

    public Timestamp getReceivedTime() {
      return this.receivedTime;
    }

    public String getOrderingKey() {
      return orderingKey;
    }

    public long getSequeunceNumber() {
      return sequeunceNumber;
    }
  }

  public MessageTracker() {
  }


  public synchronized void addSent(Iterable<MessageIdentifier> identifiers) {
    for (MessageIdentifier identifier : identifiers) {
      long client_id = identifier.getPublisherClientId();
      long sequence_number = identifier.getSequenceNumber();

      if (!sentMessages.containsKey(client_id)) {
        sentMessages.put(client_id, new HashSet<>());
      }
      sentMessages.get(client_id).add(sequence_number);
    }
  }

  public synchronized void addReceived(Iterable<MessageIdentifier> identifiers) {
    for (MessageIdentifier identifier : identifiers) {
      long publisher_client_id = identifier.getPublisherClientId();
      String subscriber_client_id = identifier.getSubscriberClientId();
      long sequence_number = identifier.getSequenceNumber();
      String orderingKey = identifier.getOrderingKey();
      Timestamp receivedTime = identifier.getReceivedTime();

      if (!receivedMessages.containsKey(publisher_client_id)) {
        receivedMessages.put(publisher_client_id, new HashMap<>());
      }
      Map<Long, Integer> publisher_id_recv_map = receivedMessages.get(publisher_client_id);
      if (!publisher_id_recv_map.containsKey(sequence_number)) {
        publisher_id_recv_map.put(sequence_number, 0);
      }
      publisher_id_recv_map.put(sequence_number, publisher_id_recv_map.get(sequence_number) + 1);
      if (StringUtils.isNotBlank(orderingKey)) {
        if (!this.subscriberToOrderingKeys.containsKey(subscriber_client_id)) {
          this.subscriberToOrderingKeys.put(subscriber_client_id, new ArrayList<>());
        }
        List<ReceivedOrderedMessage> receivedKeys = this.subscriberToOrderingKeys
            .get(subscriber_client_id);
        receivedKeys.add(new ReceivedOrderedMessage(orderingKey, receivedTime, sequence_number));
        this.subscriberToOrderingKeys.put(subscriber_client_id, receivedKeys);
      }
    }
  }

  // Get the ratio of duplicate deliveries to published messages.
  public synchronized double getDuplicateRatio() {
    long duplicates = 0;
    long size = 0;
    for (Map.Entry<Long, Map<Long, Integer>> publisher_entry : receivedMessages.entrySet()) {
      for (Map.Entry<Long, Integer> sequence_number_entry : publisher_entry.getValue().entrySet()) {
        ++size;
        if (sequence_number_entry.getValue() > 1) {
          duplicates += (sequence_number_entry.getValue() - 1);
        }
      }
    }
    return ((double) duplicates) / size;
  }

  public synchronized Iterable<MessageIdentifier> getMissing() {
    ArrayList<MessageIdentifier> missing = new ArrayList<>();
    for (Map.Entry<Long, Set<Long>> published : sentMessages.entrySet()) {
      Map<Long, Integer> received_counts =
          receivedMessages.getOrDefault(published.getKey(), new HashMap<>());
      for (Long sequence_number : published.getValue()) {
        if (received_counts.getOrDefault(sequence_number, 0) == 0) {
          missing.add(
              MessageIdentifier.newBuilder()
                  .setPublisherClientId(published.getKey())
                  .setSequenceNumber(sequence_number.intValue())
                  .build());
        }
      }
    }
    return missing;
  }

  /*
   * Returns a list of subscribers that have received messages for overlapping ordering keys violating
   * subscriber affinity.
   * */
  public synchronized Map<String, Set<String>> getOverlappingSubscribers() {
    Map<String, Set<String>> overlaps = new HashMap<>();
    if (this.subscriberToOrderingKeys.keySet().size() < 2) {
      return overlaps;
    }
    Set<Set<String>> pairwiseSubscribers = Sets
        .combinations(this.subscriberToOrderingKeys.keySet(), 2);

    for (Set<String> subscriberPair : pairwiseSubscribers) {
      List<String> subscribers = new ArrayList<>(subscriberPair);
      if (subscribers.size() != 2) {
        log.error(
            "SEVERE: Tried to calculate intersection of more than 2 subscribers when calculating ordering keys overlap.");
        System.exit(1);
      }
      Set<ReceivedOrderedMessage> received1 = new HashSet<>(
          this.subscriberToOrderingKeys.get(subscribers.get(0)));
      Set<ReceivedOrderedMessage> received2 = new HashSet<>(
          this.subscriberToOrderingKeys.get(subscribers.get(1)));
      SetView<ReceivedOrderedMessage> overlappingKeys = Sets.intersection(received1, received2);
      if (overlappingKeys.size() > 0) {
        for (ReceivedOrderedMessage overlappingKey : overlappingKeys) {
          log.error(
              "Subscriber {} and {} overlap for key {}\n Received sequences for 1:{}\n Received sequences for 2:{}\n",
              subscribers.get(0), subscribers.get(1), overlappingKey.getOrderingKey(),
              this.subscriberToOrderingKeys.get(subscribers.get(0)).stream()
                  .filter(r -> r.equals(overlappingKey))
                  .sorted(Comparator.comparing(r -> r.getSequeunceNumber()))
                  .map(r -> r.getSequeunceNumber())
                  .map(String::valueOf)
                  .reduce((s1, s2) -> s1 + "," + s2),
              this.subscriberToOrderingKeys.get(subscribers.get(1)).stream()
                  .filter(r -> r.equals(overlappingKey))
                  .sorted(Comparator.comparing(r -> r.getSequeunceNumber()))
                  .map(r -> r.getSequeunceNumber())
                  .map(String::valueOf)
                  .reduce((s1, s2) -> s1 + "," + s2));
          overlaps.compute(overlappingKey.getOrderingKey(), (key, oldSubscribers) -> {
            if (oldSubscribers == null) {
              oldSubscribers = new HashSet<String>();
            }
            for (String subscriber : subscriberPair) {
              oldSubscribers.add(subscriber + ":" + overlappingKey.getReceivedTime());
            }
            return oldSubscribers;
          });
        }
      }
    }
    return overlaps;
  }


}
