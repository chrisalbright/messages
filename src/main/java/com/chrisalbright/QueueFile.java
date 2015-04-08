package com.chrisalbright;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class QueueFile<T> implements AutoCloseable {

  public static final int HEADER_SIZE = Integer.BYTES;
  private final RandomAccessFile raf;
  private final Converter<T> converter;
  private final int maxFileSize;
  private final FileChannel channel;
  private int readPosition = 0;
  private final IntBuffer readPositionBuffer;

  public QueueFile(File raf, Converter<T> converter) throws FileNotFoundException {
    this(raf, converter, 100 * 1024);
  }

  public QueueFile(File raf, Converter<T> converter, int maxFileSize) throws FileNotFoundException {
    this.raf = new RandomAccessFile(raf, "rw");
    this.converter = converter;
    this.maxFileSize = maxFileSize;
    this.channel = this.raf.getChannel();
    try {
      readPositionBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES).asIntBuffer();
      readPosition = getReadPosition();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void commit() {
    synchronized (readPositionBuffer) {
      readPositionBuffer.put(readPosition);
      readPositionBuffer.flip();
    }
  }

  private int getReadPosition() {
    int i = readPositionBuffer.get();
    if (i < HEADER_SIZE) {
      i = HEADER_SIZE;
    }
    readPositionBuffer.flip();
    return i;
  }


  @Override
  public void close() throws IOException {
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
