package com.chrisalbright.messages.queue;

import java.io.IOException;
import java.util.function.Supplier;

final class TestSegmentWriter<T> implements Runnable {

  final Segment.Writer<T> segmentWriter;
  private final Supplier<T> messageSupplier;
  final long messageStart;
  final long messageEnd;

  TestSegmentWriter(Segment.Writer<T> segmentWriter, Supplier<T> messageSupplier, long messageStart, long messageEnd) {
    this.segmentWriter = segmentWriter;
    this.messageSupplier = messageSupplier;
    this.messageStart = messageStart;
    this.messageEnd = messageEnd;
  }


  @Override
  public void run() {
    for (Long i = messageStart; i <= messageEnd; i++) {
      try {
        segmentWriter.push(messageSupplier.get());
      } catch (IOException ignored) {
      }
    }
  }
}
