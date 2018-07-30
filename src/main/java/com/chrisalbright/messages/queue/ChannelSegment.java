package com.chrisalbright.messages.queue;

import com.google.common.base.Preconditions;

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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.attribute.PosixFilePermission.*;

public class ChannelSegment<T> implements Segment<T> {

  private final Function<T, byte[]> encoder;
  private final Function<byte[], T> decoder;
  private final int maxSegmentSize;
  private final Path dataPath;
  private final Path metaDataPath;
  private final Path recordSizePath;
  private final FileChannel dataChannel;
  private final FileChannel recordSizeChannel;
  private final MetaData metaData;

  private Lock fetchLock = new ReentrantLock();
  private Condition fetchCondition = fetchLock.newCondition();


  public ChannelSegment(Path segmentPath, Function<T, byte[]> encoder, Function<byte[], T> decoder, int maxSegmentSize) {
    this.encoder = encoder;
    this.decoder = decoder;
    this.maxSegmentSize = maxSegmentSize;
    Preconditions.checkArgument(Files.isDirectory(segmentPath), "%s must be a directory", segmentPath);

    final Set<OpenOption> openOptions = new HashSet<>();
    openOptions.add(WRITE);
    openOptions.add(APPEND);
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
      this.recordSizeChannel = FileChannel.open(recordSizePath, openOptions);
      this.metaData = new MetaData(metaDataPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public Reader<T> newReader() {
    try {
      FileChannel readChannel = FileChannel.open(dataPath, READ);
      FileChannel recordSizeChannel = FileChannel.open(recordSizePath, READ);
      return () -> {
        fetchLock.lock();
        try {
          while (metaData.getRecordCount() == 0) {
            fetchCondition.await();
          }
          ByteBuffer sizeBuf = ByteBuffer.allocate(Integer.BYTES);
          int rs = recordSizeChannel.read(sizeBuf);
          if (rs == Integer.BYTES) {
            sizeBuf.rewind();
            int size = sizeBuf.getInt();
            ByteBuffer dataBuf = ByteBuffer.allocate(size);
            readChannel.read(dataBuf);
            dataBuf.rewind();
            return java.util.Optional.ofNullable(decoder.apply(dataBuf.array()));
          } else {
            return Optional.empty();
          }
        } finally {
          fetchLock.unlock();
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Writer<T> getWriter() {
    return val -> {
      fetchLock.lock();
      try {
        ByteBuffer dataBuf = ByteBuffer.wrap(encoder.apply(val));
        ByteBuffer sizeBuf = ByteBuffer.allocate(Integer.BYTES);
        final int written = dataChannel.write(dataBuf);
        sizeBuf.putInt(written);
        sizeBuf.rewind();
        recordSizeChannel.write(sizeBuf);
        int count = metaData.incrementRecordCount();
        if (count == 0) {
          fetchCondition.signal();
        }
        return true;
      } finally {
        fetchLock.unlock();
      }
    };
  }

  @Override
  public MetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() throws Exception {

  }
}
