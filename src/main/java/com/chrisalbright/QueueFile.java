package com.chrisalbright;

import com.google.common.base.Charsets;

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

  static class Header {

    final static String MAGIC_VALUE = "MSG-1";
    final static int MAGIC_POSITION = 0;
    final static int MAGIC_LENGTH = Byte.BYTES * MAGIC_VALUE.length();
    final static int READY_FOR_DELETE_POSITION = MAGIC_POSITION + MAGIC_LENGTH;
    final static int READY_FOR_DELETE_LENGTH = Byte.BYTES;
    final static int READ_POSITION_POSITION = READY_FOR_DELETE_POSITION + READY_FOR_DELETE_LENGTH;
    final static int READ_POSITION_LENGTH = Integer.BYTES;
    final static int RECORD_COUNT_POSITION = READ_POSITION_POSITION + READ_POSITION_LENGTH;
    final static int RECORD_COUNT_LENGTH = Integer.BYTES;
    final static int STARTING_READ_POSITION = RECORD_COUNT_POSITION + RECORD_COUNT_LENGTH;

    private final ByteBuffer magic;
    private final ByteBuffer readyForDelete;
    private final IntBuffer readPosition;
    private final IntBuffer recordCount;

    public Header(FileChannel channel) throws IOException {
      long fileSize = channel.size();
      magic = channel.map(FileChannel.MapMode.READ_WRITE, MAGIC_POSITION, MAGIC_LENGTH);
      readyForDelete = channel.map(FileChannel.MapMode.READ_WRITE, READY_FOR_DELETE_POSITION, READY_FOR_DELETE_LENGTH);
      readPosition = channel.map(FileChannel.MapMode.READ_WRITE, READ_POSITION_POSITION, READ_POSITION_LENGTH).asIntBuffer();
      recordCount = channel.map(FileChannel.MapMode.READ_WRITE, RECORD_COUNT_POSITION, RECORD_COUNT_LENGTH).asIntBuffer();
      if (fileSize == 0) {
        initializeHeader();
      } else if(getMagic() != MAGIC_VALUE) {
        throw new IllegalStateException("File Header does not contain magic value");
      }
    }

    private void initializeHeader() {
      writeMagic();
      setReadPosition(STARTING_READ_POSITION);
      setRecordCount(0);
      markNotReadyForDelete();

    }

    private void writeMagic() {
      synchronized (magic) {
        magic.put(MAGIC_VALUE.getBytes(Charsets.UTF_8));
        magic.flip();
      }
    }
    public String getMagic() {
      byte[] buf = new byte[MAGIC_LENGTH];
      synchronized (magic) {
        magic.get(buf);
        magic.flip();
      }
      return new String(buf);
    }

    public void setReadPosition(int position) {
      synchronized (readPosition) {
        readPosition.put(position);
        readPosition.flip();
      }
    }
    public Integer getReadPosition() {
      synchronized (readPosition) {
        int position = readPosition.get();
        readPosition.flip();
        return position;
      }
    }

    public void setRecordCount(int position) {
      synchronized (recordCount) {
        recordCount.put(position);
        recordCount.flip();
      }
    }
    public Integer getRecordCount() {
      synchronized (recordCount) {
        int count = recordCount.get();
        recordCount.flip();
        return count;
      }
    }

    public void markReadyForDelete() {
      synchronized (readyForDelete) {
        byte b = 1;
        readyForDelete.put(b);
        readyForDelete.flip();
      }
    }
    public void markNotReadyForDelete() {
      synchronized (readyForDelete) {
        byte b = 0;
        readyForDelete.put(b);
        readyForDelete.flip();
      }
    }
    public Boolean isReadyForDelete() {
      synchronized (readyForDelete) {
        byte b = readyForDelete.get();
        readyForDelete.flip();
        return b == 1;
      }
    }
  }

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
    readPositionBuffer.put(readPosition);
    readPositionBuffer.flip();
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
    length = channel.size() - readPosition;

    ByteBuffer buffer;
    buffer = channel.map(FileChannel.MapMode.READ_ONLY, readPosition, length);

    if (buffer.limit() <= 0) {
      return Optional.empty();
    }

    int byteLength = buffer.getInt();
    byte[] data = new byte[byteLength];
    buffer.get(data);

    readPosition += buffer.position();

    return Optional.of(converter.fromBytes(data));
  }

  public boolean push(T val) throws IOException {
    byte[] bytes = converter.toBytes(val);
    ByteBuffer buffer;
    int dataSize = bytes.length + Integer.BYTES;
    long channelSize = channel.size();
    if ((channelSize + dataSize) > maxFileSize) {
      return false;
    }
    buffer = channel.map(FileChannel.MapMode.READ_WRITE, channelSize, dataSize);
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    return true;
  }

  public static interface Converter<T> {
    public byte[] toBytes(T val);

    public T fromBytes(byte[] data);
  }
}
