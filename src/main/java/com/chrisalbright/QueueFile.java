package com.chrisalbright;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class QueueFile<T> implements AutoCloseable, Iterable<T> {

  public static final int HEADER_SIZE = Integer.BYTES;
  private final RandomAccessFile raf;
  private final int maxFileSize;

  private final FileChannel channel;
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;
  private int readPosition = 0;
  private final IntBuffer readPositionBuffer;

  public QueueFile(File raf, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws FileNotFoundException {
    this(raf, encoder, decoder, 100 * 1024);
  }

  public QueueFile(File raf, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxFileSize) throws FileNotFoundException {
    this.raf = new RandomAccessFile(raf, "rw");
    this.encoderFn = encoder;
    this.decoderFn = decoder;
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

    return Optional.of(decoderFn.apply(data));
  }

  public boolean push(T val) throws IOException {
    byte[] bytes = encoderFn.apply(val);
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

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      Optional<T> next = Optional.empty();

      @Override
      public boolean hasNext() {
        try {
          next = fetch();
          return next.isPresent();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T next() {
        return next.orElse(null);
      }
    };
  }

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

    ReadWriteLock readPositionLock = new ReentrantReadWriteLock();
    ReadWriteLock recordCountLock = new ReentrantReadWriteLock();
    ReadWriteLock readyForDeleteLock = new ReentrantReadWriteLock();

    public Header(FileChannel channel) throws IOException {
      long fileSize = channel.size();
      magic = channel.map(FileChannel.MapMode.READ_WRITE, MAGIC_POSITION, MAGIC_LENGTH);
      readyForDelete = channel.map(FileChannel.MapMode.READ_WRITE, READY_FOR_DELETE_POSITION, READY_FOR_DELETE_LENGTH);
      readPosition = channel.map(FileChannel.MapMode.READ_WRITE, READ_POSITION_POSITION, READ_POSITION_LENGTH).asIntBuffer();
      recordCount = channel.map(FileChannel.MapMode.READ_WRITE, RECORD_COUNT_POSITION, RECORD_COUNT_LENGTH).asIntBuffer();
      if (fileSize == 0) {
        initializeHeader();
      } else if (getMagic() != MAGIC_VALUE) {
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
      Lock l = readPositionLock.writeLock();
      try {
        l.lock();
        readPosition.put(position);
        readPosition.flip();
      } finally {
        l.unlock();
      }
    }

    public Integer getReadPosition() {
      Lock l = readPositionLock.readLock();
      try {
        l.lock();
        int position = readPosition.get();
        readPosition.flip();
        return position;
      } finally {
        l.unlock();
      }
    }

    public void setRecordCount(int count) {
      Lock l = recordCountLock.writeLock();
      try {
        l.lock();
        recordCount.put(count);
        recordCount.flip();
      } finally {
        l.unlock();
      }
    }

    public void incrementRecordCount() {
      Lock l = recordCountLock.writeLock();
      try {
        l.lock();
        int count = recordCount.get();
        recordCount.flip();
        recordCount.put(count + 1);
        recordCount.flip();
      } finally {
        l.unlock();
      }
    }

    public Integer getRecordCount() {
      Lock l = recordCountLock.readLock();
      try {
        l.lock();
        int count = recordCount.get();
        recordCount.flip();
        return count;
      } finally {
        l.unlock();
      }
    }

    private void writeReadyForDelete(boolean b) {
      Lock l = readyForDeleteLock.writeLock();
      try {
        l.lock();
        readyForDelete.put((byte)(b ? 1 : 0));
        readyForDelete.flip();
      } finally {
        l.unlock();
      }

    }
    public void markReadyForDelete() {
      writeReadyForDelete(true);
    }

    public void markNotReadyForDelete() {
      writeReadyForDelete(false);
    }

    public Boolean isReadyForDelete() {
      Lock l = readyForDeleteLock.readLock();
      try {
        l.lock();
        byte b = readyForDelete.get();
        readyForDelete.flip();
        return b == 1;
      } finally {
        l.unlock();
      }
    }

  }

}

