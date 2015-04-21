package com.chrisalbright;

import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class QueueFileTest {

  File f;
  QueueFile<String> q;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    f = folder.newFile("queue-file");
    q = openStringQueueFile();
  }

  private QueueFile<String> openStringQueueFile() throws IOException {
    return new QueueFile<>(f, Converters.STRING_ENCODER, Converters.STRING_DECODER);
  }

  @Test
  public void testAddSingleItemToQueueFile() throws IOException, InterruptedException {
    String expected = "hello world";
    q.push(expected);

    Optional<String> optional = q.fetch();
    assertTrue(optional.isPresent());

    String actual = optional.get();
    assertThat(actual, is(expected));
  }

  @Test
  public void testAddMultipleItemsToQueueFile() throws IOException, InterruptedException {
    String expected1 = "hello world";
    String expected2 = "hello dolly";
    String expected3 = "howdee doodie";

    q.push(expected1);
    q.push(expected2);
    q.push(expected3);

    String actual1 = q.fetch().get();
    String actual2 = q.fetch().get();
    String actual3 = q.fetch().get();

    assertThat(actual1, is(expected1));
    assertThat(actual2, is(expected2));
    assertThat(actual3, is(expected3));
  }

  @Test
  public void testCRC() {
    Optional<CRC32> crc32a = Optional.of(new CRC32());
    CRC32 crc32b = new CRC32();
    Optional<CRC32> crc32c = Optional.empty();

    ByteBuffer buffer1 = ByteBuffer.allocate(16);
    buffer1.putInt(1);
    buffer1.putInt(2);
    buffer1.putInt(3);
    buffer1.putInt(4);

    ByteBuffer buffer2 = ByteBuffer.allocate(16);
    buffer2.putInt(1);
    buffer2.putInt(2);
    buffer2.putInt(3);
    buffer2.putInt(4);

    Function<CRC32, Long> mapper = (c) -> {
      c.update(buffer1);
      long sum = c.getValue();
      c.reset();
      c.getValue();
      return sum;
    };
    Long crc1 = crc32a.map(mapper).orElse(-1L);
    crc32b.update(buffer2);

    Long crc2 = crc32b.getValue();
    Long crc3 = crc32c.map(mapper).orElse(-1l);

    assertEquals(crc1, crc2);
    assertEquals(Long.valueOf(-1l), crc3);
  }

  @Test
  public void testAddAnyTypeToQueueFile() throws IOException, InterruptedException {
    QueueFile<Long> q = new QueueFile<>(folder.newFile(), Converters.LONG_ENCODER, Converters.LONG_DECODER);

    q.push(1l);
    q.push(2l);
    q.push(3l);

    Long actual1 = q.fetch().get();
    Long actual2 = q.fetch().get();
    Long actual3 = q.fetch().get();

    assertThat(actual1, is(1l));
    assertThat(actual2, is(2l));
    assertThat(actual3, is(3l));
  }

  @Test(timeout = 2000)
  public void testPerformance() throws IOException, InterruptedException {
    SecureRandom r = new SecureRandom();
    int messages = 30;
    int iterations = 10000;
    int messageSize = 1024;
    File f = folder.newFile();
    QueueFile<byte[]> q = new QueueFile<>(f, Converters.BYTE_ARRAY_ENCODER, Converters.BYTE_ARRAY_DECODER, 1000 * 1024 * 1024);
    byte[][] data = new byte[messages][messageSize];
    Stopwatch w = Stopwatch.createStarted();
    for (int i = 0; i < messages; i++) {
      r.nextBytes(data[i]);
    }
    w.stop();
    System.out.println("Generated " + messages + " random messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    w = Stopwatch.createStarted();
    for (int i = 0; i < iterations; i++) {
      q.push(data[i % messages]);
    }
    w.stop();
    System.out.println("Wrote " + iterations + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    Optional<byte[]> val = Optional.empty();
    byte[] bytes = new byte[messageSize];
    w = Stopwatch.createStarted();
    while ((val = q.fetch()) != Optional.<byte[]>empty()) {
      bytes = val.get();
    }
    w.stop();
    System.out.println("Read " + iterations + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
  }

  @Test
  public void testDoesNotExceedMaxFilesize() throws IOException {
    int messages = 99;
    int messageSize = 1024;

    File f = folder.newFile();
    QueueFile<byte[]> q = new QueueFile<>(f, Converters.BYTE_ARRAY_ENCODER, Converters.BYTE_ARRAY_DECODER, 100 * 1024);

    SecureRandom r = new SecureRandom();
    byte[] b = new byte[messageSize];

    for (int i = 0; i < messages; i++) {
      b = new byte[messageSize];
      r.nextBytes(b);
      assertTrue(q.push(b));
    }

    r.nextBytes(b);
    assertFalse(q.push(b));
  }

  @Test
  public void testIndicatesWhenFull() throws IOException {
    int messages = 100;
    int messageSize = 1024;

    File f = folder.newFile();
    QueueFile<byte[]> q = new QueueFile<>(f, Converters.BYTE_ARRAY_ENCODER, Converters.BYTE_ARRAY_DECODER, 100 * 1024);

    SecureRandom r = new SecureRandom();
    byte[] b;

    assertTrue(q.hasCapacity());
    for (int i = 0; i < messages; i++) {
      b = new byte[messageSize];
      r.nextBytes(b);
      q.push(b);
    }
    assertFalse(q.hasCapacity());
  }

  @Test
  public void testSavesReadPosition() throws IOException, InterruptedException {
    q.push("Hello World");
    q.push("Hello Dolly");

    String first = q.fetch().get();

    q.commit();
    q.close();

    q = openStringQueueFile();

    String second = q.fetch().get();

    assertThat(second, is("Hello Dolly"));
  }

  @Test
  public void testSavesRecordsConsumed() throws IOException, InterruptedException {
    q.push("Hello World");
    q.push("Hello Dolly");
    q.push("What's up doc?");

    assertThat(q.getRecordCount(), is(3));

    String first = q.fetch().get();

    q.commit();

    assertThat(q.getRecordCount(), is(2));
  }

  @Test
  public void testQueueFileHeaderDefaults() throws IOException {
    RandomAccessFile file = new RandomAccessFile(folder.newFile("headerFile"), "rw");
    QueueFile.Header h = new QueueFile.Header(file.getChannel());
    assertThat(h.getMagic(), is(QueueFile.Header.MAGIC_VALUE));
    assertThat(h.getReadPosition(), is(QueueFile.Header.STARTING_READ_POSITION));
    assertThat(h.getRecordCount(), is(0));
    assertThat(h.getConsumedCount(), is(0));
    assertThat(h.isReadyForDelete(), is(false));
    assertThat(h.hasCapacity(), is(true));

  }

  @Test
  public void testQueueFileHeader() throws IOException {
    RandomAccessFile file = new RandomAccessFile(folder.newFile("headerFile"), "rw");
    QueueFile.Header h = new QueueFile.Header(file.getChannel());

    h.setReadPosition(75);
    assertThat(h.getReadPosition(), is(75));

    h.setRecordCount(99);
    assertThat(h.getRecordCount(), is(99));

    h.setConsumedCount(100);
    assertThat(h.getConsumedCount(), is(100));
    h.incrementRecordCount();
    assertThat(h.getRecordCount(), is(100));

    h.markReadyForDelete();
    assertThat(h.isReadyForDelete(), is(true));

    h.markNotReadyForDelete();
    assertThat(h.isReadyForDelete(), is(false));

    h.setNoCapacity();
    assertThat(h.hasCapacity(), is(false));
  }

  @Test
  public void testAddToQueueIncrementsRecordCount() throws IOException {

    assertThat(q.getRecordCount(), is(0));

    q.push("hello");
    q.push("world");

    int recordCount = q.getRecordCount();
    assertThat(recordCount, is(2));
  }

  @Test
  public void testPeekPreservesAllElements() throws IOException, InterruptedException {
    assertThat(q.getRecordCount(), is(0));

    q.push("hello");
    q.push("world");

    assertThat(q.peek().get(), is("hello"));
    int recordCount = q.getRecordCount();
    assertThat(recordCount, is(2));

  }

  @Test
  public void testBlocksOnEmptyQueue() throws IOException, InterruptedException {

    Stopwatch timer = Stopwatch.createStarted();
    new Thread(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(300);
        q.push("Hello world");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).start();

    q.fetch();
    timer.stop();

    assertThat(timer.elapsed(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(300L));
  }

  @Test(expected = IllegalStateException.class)
  public void testQueueFileHeaderWillNotOverwriteExistingFile() throws IOException {
    RandomAccessFile file = new RandomAccessFile(folder.newFile("headerFile"), "rw");
    file.writeBytes("hello world");
    new QueueFile.Header(file.getChannel());
  }

  @Test
  public void testQueueIsIterable() throws IOException {
    q.push("one");
    q.push("two");
    q.push("three");
    q.push("four");
    q.push("five");

    Iterator<String> iterator = q.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("one"));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("two"));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("three"));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("four"));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("five"));
    assertFalse(iterator.hasNext());
  }

}
