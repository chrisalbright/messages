package com.chrisalbright;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

public final class Converters {
  private Converters() {
  }

  public static final QueueFile.Converter<byte[]> BYTE_ARRAY_CONVERTER = new QueueFile.Converter<byte[]>() {
    @Override
    public byte[] toBytes(byte[] val) {
      return val;
    }

    @Override
    public byte[] fromBytes(byte[] data) {
      return data;
    }
  };
  public static final QueueFile.Converter<Long> LONG_CONVERTER = new QueueFile.Converter<Long>() {
    @Override
    public byte[] toBytes(Long val) {
      return Longs.toByteArray(val);
    }

    @Override
    public Long fromBytes(byte[] data) {
      return Longs.fromByteArray(data);
    }
  };
  public static final QueueFile.Converter<String> STRING_CONVERTER = new QueueFile.Converter<String>() {
    @Override
    public byte[] toBytes(String val) {
      return val.getBytes(Charsets.UTF_8);
    }

    @Override
    public String fromBytes(byte[] data) {
      return new String(data);
    }
  };
}
