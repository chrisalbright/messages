package com.chrisalbright;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;
import java.util.function.Function;

public class QueueFile<T> implements AutoCloseable, Iterable<T> {

  private final RandomAccessFile raf;
  private final FileChannel channel;
  private final String fileName;

  private final int maxFileSize;

  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;

  private Lock pushLock = new ReentrantLock();
  private Lock fetchLock = new ReentrantLock();
  private Condition fetchCondition = fetchLock.newCondition();

  private final Header header;
  private int readPosition = 0;
  private AtomicInteger consumedRecordCount;

  public QueueFile(File file, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws IOException {
    this(file, encoder, decoder, 100 * 1024);
  }

  public QueueFile(File file, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxFileSize) throws IOException {
    this.raf = new RandomAccessFile(file, "rw");
    this.encoderFn = encoder;
    this.decoderFn = decoder;
    this.maxFileSize = maxFileSize;
    this.channel = this.raf.getChannel();
    this.header = new Header(channel);
    this.readPosition = header.getReadPosition();
    this.fileName = file.getName();
    this.consumedRecordCount = new AtomicInteger(0);
  }

  public synchronized void commit() {
    header.setReadPosition(readPosition);
    header.setConsumedCount(consumedRecordCount.get());
  }

  public int getReadPosition() {
    return header.getReadPosition();
  }

  public int getRecordCount() {
    return header.getRecordCount() - header.getConsumedCount();
  }

  @Override
  public void close() throws IOException {
    this.channel.close();
    this.raf.close();
  }

  public Optional<T> peek() throws IOException, InterruptedException {
    try {
      fetchLock.lock();
      long length = channel.size() - readPosition;

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

      return Optional.of(decoderFn.apply(data));
    } finally {
      fetchLock.unlock();
    }
  }

  public Optional<T> fetch() throws IOException, InterruptedException {
    try {
      fetchLock.lock();

      long length = channel.size() - readPosition;

      while (header.getRecordCount() == 0) {
        fetchCondition.await();
      }

      ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, readPosition, length);

      if (buffer.limit() <= 0) {
        return Optional.empty();
      }

      int byteLength = buffer.getInt();
      byte[] data = new byte[byteLength];
      buffer.get(data);

      readPosition += buffer.position();

      consumedRecordCount.incrementAndGet();
      return Optional.of(decoderFn.apply(data));
    } finally {
      fetchLock.unlock();
    }
  }


  public boolean push(T val) throws IOException {
    try {
      pushLock.lock();
      byte[] bytes = encoderFn.apply(val);
      int dataSize = bytes.length + Integer.BYTES;
      if (dataSize > maxFileSize) {
        throw new IOException("Attempt to write a message larger than max file size.");
      }
      long channelSize = channel.size();
      if ((channelSize + dataSize) > maxFileSize) {
        header.setNoCapacity();
        return false;
      }
      ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channelSize, dataSize);
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

  public String getFileName() {
    return fileName;
  }

  static class Header {

    final static String MAGIC_VALUE = "MSG1";
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
    final static int CONSUMED_RECORD_COUNT_POSITION = RECORD_COUNT_POSITION + RECORD_COUNT_SIZE;
    final static int CONSUMED_RECORD_COUNT_SIZE = Integer.BYTES;
    final static int STARTING_READ_POSITION = CONSUMED_RECORD_COUNT_POSITION + CONSUMED_RECORD_COUNT_SIZE;
    final static int LENGTH = STARTING_READ_POSITION - 1;

    private final ByteBuffer magic;
    private final ByteBuffer readyForDelete;
    private final ByteBuffer hasCapacity;
    private final IntBuffer readPosition;
    private final IntBuffer recordCount;
    private final IntBuffer consumedRecordCount;

    ReadWriteLock readyForDeleteLock = new ReentrantReadWriteLock();
    ReadWriteLock hasCapacityLock = new ReentrantReadWriteLock();
    ReadWriteLock readPositionLock = new ReentrantReadWriteLock();
    ReadWriteLock recordCountLock = new ReentrantReadWriteLock();
    ReadWriteLock consumedRecordCountLock = new ReentrantReadWriteLock();

    public Header(FileChannel channel) throws IOException {
      long fileSize = channel.size();
      magic = channel.map(FileChannel.MapMode.READ_WRITE, MAGIC_POSITION, MAGIC_SIZE);
      readyForDelete = channel.map(FileChannel.MapMode.READ_WRITE, READY_FOR_DELETE_POSITION, READY_FOR_DELETE_SIZE);
      hasCapacity = channel.map(FileChannel.MapMode.READ_WRITE, HAS_CAPACTIY_POSITION, HAS_CAPACTIY_SIZE);
      readPosition = channel.map(FileChannel.MapMode.READ_WRITE, READ_POSITION_POSITION, READ_POSITION_SIZE).asIntBuffer();
      recordCount = channel.map(FileChannel.MapMode.READ_WRITE, RECORD_COUNT_POSITION, RECORD_COUNT_SIZE).asIntBuffer();
      consumedRecordCount = channel.map(FileChannel.MapMode.READ_WRITE, CONSUMED_RECORD_COUNT_POSITION, CONSUMED_RECORD_COUNT_SIZE).asIntBuffer();
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
      try {
        l.lock();
        readPosition.put(position);
        readPosition.flip();
      } finally {
        l.unlock();
      }
    }

    public int getReadPosition() {
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
      try {
        l.lock();
        int count = recordCount.get();
        recordCount.flip();
        return count;
      } finally {
        l.unlock();
      }
    }

    public int getConsumedCount() {
      return LockUtil.withLock(consumedRecordCountLock.readLock(), () -> {
        int count = consumedRecordCount.get();
        consumedRecordCount.flip();
        return count;
      });
    }

    public void setConsumedCount(final int count) {
      LockUtil.withLock(consumedRecordCountLock.writeLock(), () -> {
        consumedRecordCount.put(count);
        consumedRecordCount.flip();
        return Void.TYPE;
      });
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
