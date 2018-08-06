package com.chrisalbright.messages.queue;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.attribute.PosixFilePermission.*;

public class MappedSegment<T> implements Segment<T> {

  private final int maxSegmentSize;

  private final MetaData metaData;
  private final Function<T, byte[]> encoder;
  private final Function<byte[], T> decoder;
  private final Path recordSizePath;
  private final Path metaDataPath;
  private final FileChannel dataChannel;
  private final FileChannel recordLengthChannel;

  private Lock pushLock = new ReentrantLock();
  private Lock fetchLock = new ReentrantLock();
  private Condition fetchCondition = fetchLock.newCondition();

  private Path dataPath;

  public MappedSegment(File dataPath, Function<T, byte[]> encoder, Function<byte[], T> decoder) throws IOException {
    this(dataPath, encoder, decoder, 100 * 1024 * 1024);
  }

  public MappedSegment(File dataPath, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxSegmentSize) throws IOException {
    this(dataPath.toPath(), encoder, decoder, maxSegmentSize);
  }

  public MappedSegment(Path segmentPath, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxSegmentSize) throws IOException {
    this.encoder = encoder;
    this.decoder = decoder;
    this.maxSegmentSize = maxSegmentSize;
    Preconditions.checkArgument(Files.isDirectory(segmentPath), "%s must be a directory", segmentPath);

    final Set<OpenOption> openOptions = new HashSet<>();
    openOptions.add(WRITE);
    openOptions.add(READ);
    openOptions.add(CREATE);
    openOptions.add(DSYNC);

    final Set<PosixFilePermission> permissions = new HashSet<>();
    permissions.add(OWNER_READ);
    permissions.add(OWNER_WRITE);
    permissions.add(GROUP_READ);

    final FileAttribute<Set<PosixFilePermission>> fileAttribute = PosixFilePermissions.asFileAttribute(permissions);

    try {
      Path p = Files.createDirectories(segmentPath, fileAttribute);
      Preconditions.checkArgument(Files.exists(p), "Unable to create %s", p);
      this.dataPath = segmentPath.resolve("segment.data");
      this.recordSizePath = segmentPath.resolve("segment.recordsize");
      this.metaDataPath = segmentPath.resolve("segment.meta");
      this.dataChannel = FileChannel.open(dataPath, openOptions);
      this.recordLengthChannel = FileChannel.open(recordSizePath, openOptions);
      this.metaData = new MetaData(metaDataPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void close() throws IOException {
    this.dataChannel.close();
  }

  private void signalNotEmpty() {
    boolean signalSent;
    do {
      try {
        while (true) {
          if (fetchLock.tryLock(100, TimeUnit.MILLISECONDS)) break;
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
        int dataReadPosition = 0;
        int recordSizeReadPosition = 0;
        FileChannel dataChannel = FileChannel.open(dataPath, READ, WRITE);
        FileChannel recordSizeChannel = FileChannel.open(recordSizePath, READ, WRITE);

        @Override
        public Optional<T> fetch() throws IOException, InterruptedException {
          try {
            fetchLock.lock();

            while (metaData.getRecordCount() == 0) {
              fetchCondition.await(100, TimeUnit.MILLISECONDS);
            }
            ByteBuffer recordSizeBuffer = recordSizeChannel.map(FileChannel.MapMode.READ_ONLY, recordSizeReadPosition, Integer.BYTES);
            int dataLength = recordSizeBuffer.getInt();
            ByteBuffer dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, dataReadPosition, dataLength);

            if (dataBuffer.limit() <= 0) {
              return Optional.empty();
            }

            byte[] data = new byte[dataLength];
            dataBuffer.get(data);

            dataReadPosition += dataBuffer.position();
            recordSizeReadPosition += Integer.BYTES;

            return Optional.of(decoder.apply(data));
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
        byte[] bytes = encoder.apply(val);
        long dataChannelSize = dataChannel.size();
        long recordLengthChannelSize = recordLengthChannel.size();
        if ((dataChannelSize + bytes.length) > maxSegmentSize) {
          return false;
        }
        ByteBuffer dataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, dataChannelSize, bytes.length);
        dataBuffer.put(bytes);
        ByteBuffer sizeBuffer = recordLengthChannel.map(FileChannel.MapMode.READ_WRITE, recordLengthChannelSize, Integer.BYTES);
        sizeBuffer.putInt(bytes.length);
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
