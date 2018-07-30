package com.chrisalbright.messages.queue;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class MappedSegment<T> implements Segment<T> {

  private final RandomAccessFile raf;
  private final int maxFileSize;

  private final FileChannel channel;
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;
  private final MetaData metaData;

  private Lock pushLock = new ReentrantLock();
  private Lock fetchLock = new ReentrantLock();
  private Condition fetchCondition = fetchLock.newCondition();

  private final Header header;
  private File path;
  private long id;

  public MappedSegment(long id, File path, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws IOException {
    this(id, path, encoder, decoder, 100 * 1024);
  }

  public MappedSegment(long id, File path, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxFileSize) throws IOException {
    Preconditions.checkArgument(path.isDirectory(), "%s must be a directory", path);
    if (!path.mkdirs()) {
      final boolean pathExistsOrWasCreated = path.exists();
      Preconditions.checkArgument(pathExistsOrWasCreated, "Unable to create %s", path);
    }

    this.id = id;
    this.path = path;
    this.raf = openSegmentFile(this.path, this.id);
    this.encoderFn = encoder;
    this.decoderFn = decoder;
    this.maxFileSize = maxFileSize;
    this.metaData = new MetaData(new File(path.getCanonicalPath().concat(".meta")).toPath());
    this.header = new Header(metaData);
    this.channel = raf.getChannel();
  }

  private RandomAccessFile openSegmentFile(File path, long id) throws FileNotFoundException {
    return new RandomAccessFile(fileFor(path, FileType.segment, this.id), "rwd");
  }

  private File fileFor(File path, FileType fileType, long id) {
    return new File(path, String.format("%020d.%s", this.id, fileType));
  }

  @Override
  public void close() throws IOException {
    this.channel.close();
    this.raf.close();
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
  public Reader<T> newReader() {

    try {
      return new Reader<T>() {
        int readPosition = 0;
        FileChannel channel = openSegmentFile(path, id).getChannel();

        @Override
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
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Writer<T> getWriter() {
    return val -> {
      try {
        pushLock.lock();
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
        int count = header.incrementRecordCount();
        if (count == 0) {
          signalNotEmpty();
        }
        return true;
      } finally {
        pushLock.unlock();
      }
    };
  }

  @Override
  public MetaData getMetaData() {
    return metaData;
  }

  static class Header {

    private final MetaData metaData;

    Header(MetaData metaData) {
      this.metaData = metaData;
    }

    public int incrementRecordCount() {
      return metaData.incrementRecordCount();
    }

    public int getRecordCount() throws IOException {
      return metaData.getRecordCount();
    }

    private static void writeBoolean(ByteBuffer buffer, Lock l, boolean value) {
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

  }

}
