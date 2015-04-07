package com.chrisalbright;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class QueueFile<T> implements AutoCloseable {

  private final RandomAccessFile raf;
  private final Converter<T> converter;
  private final int maxFileSize;
  private final FileChannel channel;
  private int readPosition = 0;

  public QueueFile(RandomAccessFile raf, Converter<T> converter) {
    this(raf, converter, 100 * 1024);
  }

  public QueueFile(RandomAccessFile raf, Converter<T> converter, int maxFileSize) {
    this.raf = raf;
    this.converter = converter;
    this.maxFileSize = maxFileSize;
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

    ByteBuffer buffer;
    synchronized (this) {
      buffer = channel.map(FileChannel.MapMode.READ_ONLY, readPosition, length);
    }

    if (buffer.limit() <= 0) {
      return Optional.empty();
    }

    int byteLength = buffer.getInt();
    byte[] data = new byte[byteLength];
    buffer.get(data);

    synchronized (this) {
      readPosition += buffer.position();
    }

    return Optional.of(converter.fromBytes(data));
  }

  public boolean push(T val) throws IOException {
    byte[] bytes = converter.toBytes(val);
    ByteBuffer buffer;
    synchronized (channel) {
      int dataSize = bytes.length + Integer.BYTES;
      long channelSize = channel.size();
      if ((channelSize + dataSize) > maxFileSize) {
        return false;
      }
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, channelSize, dataSize);
    }
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    return true;
  }

  public static interface Converter<T> {
    public byte[] toBytes(T val);

    public T fromBytes(byte[] data);
  }
}
