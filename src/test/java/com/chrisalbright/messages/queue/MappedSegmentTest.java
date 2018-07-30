package com.chrisalbright.messages.queue;

import com.chrisalbright.messages.Converters;

import java.io.File;
import java.io.IOException;

public class MappedSegmentTest extends StringSegmentTest {

  @Override
  public Segment<String> openNewSegment(File segmentFile) {
    try {
      return new MappedSegment<>(Long.MIN_VALUE, segmentFile, Converters.STRING_ENCODER, Converters.STRING_DECODER, 10 * 1024 * 1024);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}
