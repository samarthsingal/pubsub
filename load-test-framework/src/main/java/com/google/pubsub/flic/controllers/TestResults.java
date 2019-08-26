package com.google.pubsub.flic.controllers;

import com.google.pubsub.flic.controllers.ClientType.MessagingSide;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public class TestResults {

  private Map<MessagingSide, Pair<List<String>, List<String>>> invariantResults;

  public Map<MessagingSide, Pair<List<String>, List<String>>> getInvariantResults() {
    return invariantResults;
  }

  protected void setInvariantResults(
      Map<MessagingSide, Pair<List<String>, List<String>>> invariantResults) {
    this.invariantResults = invariantResults;
  }

  @Override
  public String toString() {
    return "TestResults{" +
        "invariantResults=" + invariantResults +
        '}';
  }
}
