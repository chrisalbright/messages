package com.chrisalbright.messages.queue;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.locks.*;
import java.util.function.Function;

public class Segment<T> implements AutoCloseable, Iterable<T> {

  private final RandomAccessFile raf;
  private final int maxFileSize;

  private final FileChannel channel;
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;
  private int readPosition = 0;

  private Lock pushLock = new ReentrantLock();
  private Lock fetchLock = new ReentrantLock();
  private Condition fetchCondition = fetchLock.newCondition();

  private final Header header;

  public Segment(File raf, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws IOException {
    this(raf, encoder, decoder, 100 * 1024);
  }

  public Segment(File raf, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxFileSize) throws IOException {
    this.raf = new RandomAccessFile(raf, "rw");
    this.encoderFn = encoder;
    this.decoderFn = decoder;
    this.maxFileSize = maxFileSize;
    this.channel = this.raf.getChannel();
    header = new Header(channel);
    readPosition = header.getReadPosition();
  }

  public void commit() {
    header.setReadPosition(readPosition);
  }

  public int getReadPosition() {
    return header.getReadPosition();
  }

  public int getRecordCount() {
    return header.getRecordCount();
  }

  @Override
  public void close() throws IOException {
    this.channel.close();
    this.raf.close();
  }

  public Optional<T> fetch() throws IOException, InterruptedException {
    try {
      fetchLock.lock();

      long length = 0;
      length = channel.size() - readPosition;

      while (header.getRecordCount() == 0) {
        fetchCondition.await();
      }

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
    } finally {
      fetchLock.unlock();
    }
  }


  public boolean push(T val) throws IOException {
    try {
      pushLock.lock();
      byte[] bytes = encoderFn.apply(val);
      ByteBuffer buffer;
      int dataSize = bytes.length + Integer.BYTES;
      long channelSize = channel.size();
      if ((channelSize + dataSize) > maxFileSize) {
        header.setNoCapacity();
        return false;
      }
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, channelSize, dataSize);
      buffer.putInt(bytes.length);
      buffer.put(bytes);
      int count = header.incrementRecordCount();
      if (count == 0) {
        signalNotEmpty();
      }
      return true;
    } finally {
      pushLock.unlock();
    }
  }

  private void signalNotEmpty() {
    try {
      fetchLock.lock();
      fetchCondition.signal();
    } finally {
      fetchLock.unlock();
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      Optional<T> next = Optional.empty();
      boolean gotNext = true;

      @Override
      public boolean hasNext() {
        try {
          if (gotNext) {
            next = fetch();
            gotNext = false;
          }
          return next.isPresent();
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T next() {
        gotNext = true;
        return next.orElse(null);
      }
    };
  }

  public boolean hasCapacity() {
    return header.hasCapacity();
  }

  public int recordCount() {
    return header.getRecordCount();
  }

  static class Header {

    final static String MAGIC_VALUE = "MSG-1";
    final static int MAGIC_POSITION = 0;
    final static int MAGIC_SIZE = Byte.BYTES * MAGIC_VALUE.length();
    final static int READY_FOR_DELETE_POSITION = MAGIC_POSITION + MAGIC_SIZE;
    final static int READY_FOR_DELETE_SIZE = Byte.BYTES;
    final static int HAS_CAPACTIY_POSITION = READY_FOR_DELETE_POSITION + READY_FOR_DELETE_SIZE;
    final static int HAS_CAPACTIY_SIZE = Byte.BYTES;
    final static int READ_POSITION_POSITION = HAS_CAPACTIY_POSITION + HAS_CAPACTIY_SIZE;
    final static int READ_POSITION_SIZE = Integer.BYTES;
    final static int RECORD_COUNT_POSITION = READ_POSITION_POSITION + READ_POSITION_SIZE;
    final static int RECORD_COUNT_SIZE = Integer.BYTES;
    final static int STARTING_READ_POSITION = RECORD_COUNT_POSITION + RECORD_COUNT_SIZE;

    private final ByteBuffer magic;
    private final ByteBuffer readyForDelete;
    private final ByteBuffer hasCapacity;
    private final IntBuffer readPosition;
    private final IntBuffer recordCount;

    ReadWriteLock readPositionLock = new ReentrantReadWriteLock();
    ReadWriteLock recordCountLock = new ReentrantReadWriteLock();
    ReadWriteLock readyForDeleteLock = new ReentrantReadWriteLock();
    ReadWriteLock hasCapacityLock = new ReentrantReadWriteLock();

    public Header(FileChannel channel) throws IOException {
      long fileSize = channel.size();
      magic = channel.map(FileChannel.MapMode.READ_WRITE, MAGIC_POSITION, MAGIC_SIZE);
      readyForDelete = channel.map(FileChannel.MapMode.READ_WRITE, READY_FOR_DELETE_POSITION, READY_FOR_DELETE_SIZE);
      hasCapacity = channel.map(FileChannel.MapMode.READ_WRITE, HAS_CAPACTIY_POSITION, HAS_CAPACTIY_SIZE);
      readPosition = channel.map(FileChannel.MapMode.READ_WRITE, READ_POSITION_POSITION, READ_POSITION_SIZE).asIntBuffer();
      recordCount = channel.map(FileChannel.MapMode.READ_WRITE, RECORD_COUNT_POSITION, RECORD_COUNT_SIZE).asIntBuffer();
      if (fileSize == 0) {
        initializeHeader();
      } else if (!getMagic().equals(MAGIC_VALUE)) {
        throw new IllegalStateException("File Header does not contain magic value");
      }
    }

    private void initializeHeader() {
      writeMagic();
      setReadPosition(STARTING_READ_POSITION);
      setRecordCount(0);
      markNotReadyForDelete();
      setHasCapacity();
    }

    private void writeMagic() {
      synchronized (magic) {
        magic.put(MAGIC_VALUE.getBytes(Charsets.UTF_8));
        magic.flip();
      }
    }

    public String getMagic() {
      byte[] buf = new byte[MAGIC_SIZE];
      synchronized (magic) {
        magic.get(buf);
        magic.flip();
      }
      return new String(buf);
    }

    public void setReadPosition(int position) {
      Lock l = readPositionLock.writeLock();
      writeInt(position, l, readPosition);
    }

    public int getReadPosition() {
      Lock l = readPositionLock.readLock();
      return readInt(l, readPosition);
    }

    public void setRecordCount(int count) {
      Lock l = recordCountLock.writeLock();
      writeInt(count, l, recordCount);
    }

    public int incrementRecordCount() {
      Lock l = recordCountLock.writeLock();
      try {
        l.lock();
        int count = recordCount.get();
        recordCount.flip();
        recordCount.put(count + 1);
        recordCount.flip();
        return count;
      } finally {
        l.unlock();
      }
    }

    public int getRecordCount() {
      Lock l = recordCountLock.readLock();
      return readInt(l, recordCount);
    }

    private void writeBoolean(ByteBuffer buffer, Lock l, boolean value) {
      try {
        l.lock();
        buffer.put((byte) (value ? 1 : 0));
        buffer.flip();
      } finally {
        l.unlock();
      }
    }

    private boolean readBoolean(ByteBuffer buffer, Lock l) {
      try {
        l.lock();
        byte b = buffer.get();
        buffer.flip();
        return b == 1;
      } finally {
        l.unlock();
      }
    }

    private void writeInt(int i, Lock l, IntBuffer buffer) {
      try {
        l.lock();
        buffer.put(i);
        buffer.flip();
      } finally {
        l.unlock();
      }
    }

    private int readInt(Lock l, IntBuffer buf) {
      try {
        l.lock();
        int i = buf.get();
        buf.flip();
        return i;
      } finally {
        l.unlock();
      }
    }

    private void writeReadyForDelete(boolean b) {
      writeBoolean(readyForDelete, readyForDeleteLock.writeLock(), b);
    }

    public void markReadyForDelete() {
      writeReadyForDelete(true);
    }

    public void markNotReadyForDelete() {
      writeReadyForDelete(false);
    }

    public Boolean isReadyForDelete() {
      return readBoolean(readyForDelete, readyForDeleteLock.readLock());
    }

    void setNoCapacity() {
      writeBoolean(hasCapacity, hasCapacityLock.writeLock(), false);
    }

    void setHasCapacity() {
      writeBoolean(hasCapacity, hasCapacityLock.writeLock(), true);
    }

    public boolean hasCapacity() {
      return readBoolean(hasCapacity, hasCapacityLock.readLock());
    }
  }

}
