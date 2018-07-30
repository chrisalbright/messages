package com.chrisalbright.messages.queue;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.attribute.PosixFilePermission.*;

public interface Segment<T> extends AutoCloseable {

  enum FileType {
    segment, metaData, recordSize
  }

  interface Reader<T> extends Iterable<T> {
    Optional<T> fetch() throws IOException, InterruptedException;

    @Override
    default Iterator<T> iterator() {
      return new Iterator<T>() {
        Optional<T> next = Optional.empty();
        boolean gotNext = true;

        @Override
        public boolean hasNext() {
          if (gotNext) {
            try {
              next = fetch();
            } catch (IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
            gotNext = false;
          }
          return next.isPresent();
        }

        @Override
        public T next() {
          gotNext = true;
          return next.orElse(null);
        }
      };
    }
  }

  interface Writer<T> {
    boolean push(T val) throws IOException;
  }

  final class MetaData {

    String MAGIC_VALUE = "message.segment:1.0.0";
    int MAGIC_POSITION = 0;
    int MAGIC_SIZE = Byte.BYTES * MAGIC_VALUE.length();
    int RECORD_COUNT_POSITION = MAGIC_POSITION + MAGIC_SIZE;
    int RECORD_COUNT_SIZE = Integer.BYTES;
    int FLAGS_POSITION = RECORD_COUNT_POSITION + RECORD_COUNT_SIZE;
    int FLAGS_SIZE = Long.SIZE;

    private Lock lock = new ReentrantLock(true);

    private final AtomicInteger recordCount = new AtomicInteger(0);

    private final FileChannel channel;

    public MetaData(Path metaDataFile) throws IOException {
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

      this.channel = FileChannel.open(metaDataFile, openOptions, fileAttribute);
      if (this.channel.size() == 0) {
        initialzeMetaData();
      } else if (!getMagic().equals(MAGIC_VALUE)) {
        throw new IllegalStateException("File Header does not contain magic value");
      }

      recordCount.set(getRecordCount());
    }

    private void writeMagic() throws IOException {
      final ByteBuffer magic = ByteBuffer.wrap(MAGIC_VALUE.getBytes(Charsets.UTF_8));
      magic.flip();
      channel.write(magic, MAGIC_POSITION);
    }

    private String getMagic() throws IOException {
      ByteBuffer buf = ByteBuffer.allocate(MAGIC_VALUE.getBytes(Charsets.UTF_8).length);
      channel.read(buf, MAGIC_POSITION);
      return new String(buf.array());
    }

    private void initialzeMetaData() throws IOException {
      lock.lock();
      try {
        final long[] flags = new long[1];
        final BitSet bitSet = BitSet.valueOf(flags);
        writeMagic();
        setRecordCount(0);
        Arrays.fill(flags, 0);
        setFlags(bitSet);
      } finally {
        lock.unlock();
      }
    }

    private void setFlags(BitSet bitSet) throws IOException {
      final ByteBuffer flagBuffer = ByteBuffer.allocate(FLAGS_SIZE);
      flagBuffer.asLongBuffer();
      final long[] flagsArray = bitSet.toLongArray();
      final long flags = flagsArray.length > 0 ? flagsArray[0] : 0;
      flagBuffer.putLong(flags);
      flagBuffer.rewind();
      channel.write(flagBuffer, FLAGS_POSITION);
    }

    private BitSet getFlags() throws IOException {
      final ByteBuffer flagBuffer = ByteBuffer.allocate(Long.BYTES);
      channel.read(flagBuffer, FLAGS_POSITION);
      flagBuffer.flip();
      return BitSet.valueOf(flagBuffer);
    }

    private void setRecordCount(int i) throws IOException {
      lock.lock();
      try {
      } finally {
        lock.unlock();
      }
    }

    int getRecordCount() {
      return recordCount.get();
    }

    int incrementRecordCount() {
      try {
        final ByteBuffer recordCountBuffer = ByteBuffer.allocate(Integer.BYTES);
        int count = recordCount.incrementAndGet();
        recordCountBuffer.putInt(count).flip();
        channel.write(recordCountBuffer, RECORD_COUNT_POSITION);
        return count-1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  Reader<T> newReader();

  Writer<T> getWriter();

  MetaData getMetaData();

}
