package com.google.pubsub.flic.common;

import static com.google.pubsub.flic.controllers.ClientType.MessagingSide.SUBSCRIBER;

import com.google.common.collect.Sets;
import com.google.pubsub.flic.common.LoadtestProto.FailedInvariants;
import com.google.pubsub.flic.common.LoadtestProto.SubscriberOptions.SubscriberProperties;
import com.google.pubsub.flic.controllers.ClientType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/*
 * Tracks the status of subscriber/publisher invariants/
 * Subscriber invariants specified in LoadTest.SubscriberOptions.SubscriberProperties.
 * */
public class InvariantTracker {

  private final Set<SubscriberProperties> requiredSubscriberInvariants;
  private Set<SubscriberProperties> failedSubscriberProperties;

  public InvariantTracker() {
    this.requiredSubscriberInvariants = new HashSet<>();
    this.failedSubscriberProperties = new HashSet<>();
  }

  public void addSubscriberInvariant(SubscriberProperties property) {
    this.requiredSubscriberInvariants.add(property);
  }

  public void recordViolatedInvariants(FailedInvariants violatedInvariants) {
    this.failedSubscriberProperties.addAll(
        violatedInvariants.getFailedSubscriberPropertiesList().stream()
            .collect(Collectors.toSet()));
  }

  public Map<ClientType.MessagingSide, Pair<List<String>, List<String>>> getStatus() {
    // client type -> (held invariants, violated invariants)
    Map<ClientType.MessagingSide, Pair<List<String>, List<String>>> results = new HashMap<>();

    List<String> subscriberInvariantsHeld =
        Sets.difference(requiredSubscriberInvariants, failedSubscriberProperties).stream()
        .map(SubscriberProperties::toString)
        .collect(Collectors.toList());
    List<String> subscriberInvariantsViolated = failedSubscriberProperties.stream()
        .map(SubscriberProperties::toString)
        .collect(Collectors.toList());

    results.put(SUBSCRIBER, Pair.of(subscriberInvariantsHeld, subscriberInvariantsViolated));
    return results;
  }

}
