package com.chrisalbright;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

import java.util.function.Function;

public final class Converters {
  private Converters() {
  }

  public static final Function<byte[], byte[]> BYTE_ARRAY_ENCODER = (byte[] val) -> val;
  public static final Function<byte[], byte[]> BYTE_ARRAY_DECODER = (byte[] val) -> val;

  public static final Function<Long, byte[]> LONG_ENCODER = Longs::toByteArray;
  public static final Function<byte[], Long> LONG_DECODER = Longs::fromByteArray;

  public static final Function<String, byte[]> STRING_ENCODER = (String val) -> val.getBytes(Charsets.UTF_8);
  public static final Function<byte[], String> STRING_DECODER = String::new;
}
