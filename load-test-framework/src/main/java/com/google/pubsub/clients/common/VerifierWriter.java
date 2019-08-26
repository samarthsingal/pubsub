package com.google.pubsub.clients.common;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;

public class VerifierWriter {

  private File verifierOutput;
  private FileWriter verifierWriter;
  private BufferedWriter verifierBWriter;

  public VerifierWriter(String fileName) {
    try {
      this.verifierOutput = new File(fileName);
      this.verifierOutput.delete();
      this.verifierOutput.createNewFile();
      this.verifierWriter = new FileWriter(this.verifierOutput);
      this.verifierBWriter = new BufferedWriter(this.verifierWriter);
    } catch (IOException e) {
      System.out.println("Could not create verifier output: " + e);
      System.exit(1);
    }
  }

  public synchronized void write(String orderingKey, int sequenceNum) {
    try {
      verifierBWriter.write(
          "\"" + orderingKey + "\",\"message" + sequenceNum + "\"" + "," + LocalDateTime.now()
              .toString());
      verifierBWriter.newLine();
    } catch (IOException e) {
      System.out.println("Failed to write published message: " + e);
      System.exit(1);
    }
  }

  public synchronized void shutdown() {
    try {
      verifierBWriter.flush();
      verifierBWriter.close();
    } catch (IOException e) {
      System.out.println("Failed to close: " + e);
    }
  }
}
