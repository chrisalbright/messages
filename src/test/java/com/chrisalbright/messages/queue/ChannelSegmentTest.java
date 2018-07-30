package com.chrisalbright.messages.queue;

import com.chrisalbright.messages.Converters;

import java.io.File;

public class ChannelSegmentTest extends StringSegmentTest {

  @Override
  public Segment<String> openNewSegment(File segmentPath) {
    return new ChannelSegment<>(segmentPath.toPath(), Converters.STRING_ENCODER, Converters.STRING_DECODER, 10 * 1024 * 1024);
  }

}
