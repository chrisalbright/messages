package com.chrisalbright.messages.queue;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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

  private Path path;

  public MappedSegment(File path, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws IOException {
    this(path, encoder, decoder, 100 * 1024 * 1024);
  }

  public MappedSegment(File path, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxFileSize) throws IOException {
    Preconditions.checkArgument(path.isDirectory(), "%s must be a directory", path);
    if (!path.mkdirs()) {
      final boolean pathExistsOrWasCreated = path.exists();
      Preconditions.checkArgument(pathExistsOrWasCreated, "Unable to create %s", path);
    }

    this.path = path.toPath();
    this.raf = openSegmentFile(path);
    this.encoderFn = encoder;
    this.decoderFn = decoder;
    this.maxFileSize = maxFileSize;
    this.metaData = new MetaData(new File(path.getCanonicalPath().concat(".meta")).toPath());
    this.channel = raf.getChannel();
  }

  private RandomAccessFile openSegmentFile(File path) throws FileNotFoundException {
    return new RandomAccessFile(fileFor(path, FileType.segment), "rwd");
  }

  private File fileFor(File path, FileType fileType) {
    return new File(path, String.format("segment.%s", fileType));
  }

  private Path fileFor(Path path, FileType fileType) {
    return path.resolve(String.format("segment.%s", fileType));
  }

  @Override
  public void close() throws IOException {
    this.channel.close();
    this.raf.close();
  }

  private void signalNotEmpty() {
    boolean signalSent;
    do {
      try {
        while (!fetchLock.tryLock(100, TimeUnit.MILLISECONDS)) {
        }
        try {
          fetchCondition.signal();
          signalSent = true;
        } finally {
          fetchLock.unlock();
        }
      } catch (InterruptedException e) {
        signalSent = false;
      }
    } while (!signalSent);
  }

  @Override
  public Reader<T> newReader() {

    try {
      return new Reader<T>() {
        int readPosition = 0;
        FileChannel channel = FileChannel.open(fileFor(path, FileType.segment), StandardOpenOption.READ);

        @Override
        public Optional<T> fetch() throws IOException, InterruptedException {
          try {
            fetchLock.lock();

            long length = 0;
            length = channel.size() - readPosition;

            while (metaData.getRecordCount() == 0) {
              fetchCondition.await(100, TimeUnit.MILLISECONDS);
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
        int count = metaData.incrementRecordCount();
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

}
