package com.chrisalbright;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.AutoCloseable;import java.lang.Exception;import java.lang.Integer;import java.lang.Override;import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class QueueFile<T> implements AutoCloseable {

  private final RandomAccessFile raf;
  private final Converter<T> converter;
  private final FileChannel channel;
  private int readPosition = 0;

  public QueueFile(RandomAccessFile raf, Converter<T> converter) {
    this.raf = raf;
    this.converter = converter;
    this.channel = this.raf.getChannel();
  }

  @Override
  public void close() throws Exception {
    this.channel.close();
    this.raf.close();
  }

  public Optional<T> fetch() throws IOException {
    long length = 0;
    synchronized (channel) {
      length = channel.size() - readPosition;
    }

    ByteBuffer mb = channel.map(FileChannel.MapMode.READ_ONLY, readPosition, length);
    if (mb.limit() <= 0) {
      return Optional.empty();
    }

    int byteLength = mb.getInt();
    byte[] data = new byte[byteLength];
    mb.get(data);

    synchronized (this) {
      readPosition += mb.position();
    }

    return Optional.of(converter.fromBytes(data));
  }

  public void push(T val) throws IOException {
    byte[] bytes = converter.toBytes(val);
    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), bytes.length + Integer.BYTES);
    buffer.putInt(bytes.length);
    buffer.put(bytes);
  }

  public static interface Converter<T> {
    public byte[] toBytes(T val);
    public T fromBytes(byte[] data);
  }
}
