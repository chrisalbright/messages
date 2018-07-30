package com.chrisalbright.messages.queue;

import java.io.IOException;

final class TestSegmentReader<T> implements Runnable {

  final Segment.Reader<T> reader;

  long messageCount = 0;

  TestSegmentReader(Segment<T> segment) {
    this.reader = segment.newReader();
  }

  @Override
  public void run() {
    try {
      while (reader.fetch().isPresent()) {
        messageCount++;
      }
    } catch (IOException | InterruptedException ignored) {
    }
  }
}
