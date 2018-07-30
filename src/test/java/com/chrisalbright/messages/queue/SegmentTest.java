package com.chrisalbright.messages.queue;

import com.chrisalbright.messages.Converters;
import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public abstract class SegmentTest<T> {

  File segmentFile;
  private Segment<T> segment;
  private Segment.Writer<T> segmentWriter;
  private Segment.Reader<T> segmentReader;
  private Random random = new SecureRandom();

  abstract Segment<T> openNewSegment(File segmentPath);

  abstract T newExampleElement();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    segmentFile = folder.newFolder("segment-folder");
    segment = openNewSegment(segmentFile);
    segmentWriter = segment.getWriter();
    segmentReader = segment.newReader();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSegmentExpectsADirectory() throws IOException {
    File file = folder.newFile();
    openNewSegment(file);
  }

  @Test
  public void testAddSingleItemToSegment() throws IOException, InterruptedException {
    T expected = newExampleElement();
    segmentWriter.push(expected);

    Optional<T> optional = segmentReader.fetch();
    assertTrue(optional.isPresent());

    T actual = optional.get();
    assertThat(actual, is(expected));
  }


  @Test
  public void testAddMultipleItemsToSegment() throws IOException, InterruptedException {
    T expected1 = newExampleElement();
    T expected2 = newExampleElement();
    T expected3 = newExampleElement();

    segmentWriter.push(expected1);
    segmentWriter.push(expected2);
    segmentWriter.push(expected3);

    T actual1 = segmentReader.fetch().get();
    T actual2 = segmentReader.fetch().get();
    T actual3 = segmentReader.fetch().get();

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

  @Test(timeout = 30000)
  public void testPerformance() throws IOException, InterruptedException {
    int iterations = 10000;
    Stopwatch w = Stopwatch.createStarted();
    for (int i = 0; i < iterations; i++) {
      segmentWriter.push(newExampleElement());
    }
    w.stop();
    System.out.println("Wrote " + iterations + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    w = Stopwatch.createStarted();
    while (segmentReader.fetch().isPresent()) {
      // NOOP
    }
    w.stop();
    System.out.println("Read " + iterations + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
  }

//  @Test
//  public void testDoesNotExceedMaxFileSize() throws IOException {
//    int messages = 99;
//    int messageSize = 1024;
//
//    byte[] b = new byte[messageSize];
//
//    for (int i = 0; i < messages; i++) {
//      b = new byte[messageSize];
//      random.nextBytes(b);
//      assertTrue(segment.push(b));
//    }
//
//    random.nextBytes(b);
//    assertFalse(segment.push(b));
//  }

//  @Test
//  public void testSegmentIndicatesWhenFull() throws IOException {
//    int messages = 100;
//    int messageSize = 1024;
//
//    MappedSegment<byte[]> segment = new MappedSegment<>(1, segmentFile, Converters.BYTE_ARRAY_ENCODER, Converters.BYTE_ARRAY_DECODER, 100 * 1024);
//
//    SecureRandom random = new SecureRandom();
//    byte[] b;
//
//    assertTrue(segment.hasCapacity());
//    for (int i = 0; i < messages; i++) {
//      b = new byte[messageSize];
//      random.nextBytes(b);
//      segment.push(b);
//    }
//    assertFalse(segment.hasCapacity());
//  }

//  @Test
//  public void testSegmentSavesReadPosition() throws IOException, InterruptedException {
//    segment.push("Hello World");
//    segment.push("Hello Dolly");
//
//    String first = segment.fetch().get();
//
//    segment.commit();
//    segment.close();
//
//    segment = openStringSegmentFile();
//
//    String second = segment.fetch().get();
//
//    assertThat(second, is("Hello Dolly"));
//
//  }


  @Test
  public void testAddToSegmentIncrementsRecordCount() throws IOException {

    assertThat(segment.getMetaData().getRecordCount(), is(0));

    segment.getWriter().push(newExampleElement());
    segment.getWriter().push(newExampleElement());

    int recordCount = segment.getMetaData().getRecordCount();
    assertThat(recordCount, is(2));
  }

  @Test(timeout = 2000)
  public void testBlocksOnEmptySegment() throws IOException, InterruptedException {

    Stopwatch timer = Stopwatch.createStarted();
    new Thread(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(300);
        segmentWriter.push(newExampleElement());
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    }).start();

    segmentReader.fetch();
    timer.stop();

    assertThat(timer.elapsed(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(300L));
  }

//  @Test(expected = IllegalStateException.class)
//  public void testSegmentHeaderWillNotOverwriteExistingFile() throws IOException {
//    RandomAccessFile file = new RandomAccessFile(folder.newFile("headerFile"), "rw");
//    file.writeBytes("hello world");
//    new MappedSegment.Header(file.getChannel());
//  }

  @Test
  public void testSegmentReaderIsIterable() throws IOException {
    final T one = newExampleElement();
    final T two = newExampleElement();
    final T three = newExampleElement();
    final T four = newExampleElement();
    final T five = newExampleElement();
    segmentWriter.push(one);
    segmentWriter.push(two);
    segmentWriter.push(three);
    segmentWriter.push(four);
    segmentWriter.push(five);

    Iterator<T> iterator = segmentReader.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is(one));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is(two));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is(three));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is(four));
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is(five));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testSegmentKnowsHowManyRecordsAreInIt() throws IOException {
    segment.getWriter().push(newExampleElement());
    segment.getWriter().push(newExampleElement());

    assertThat("Segment should have 2 records", segment.getMetaData().getRecordCount(), is(2));
  }

  @Test(timeout = 20000)
  public void testSegmentWriterCanHandleWritesByMultipleThreads() throws InterruptedException, IOException {
    Long messagesPerThread = 100L;
    final int numThreads = 100;
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(new TestSegmentWriter<>(segmentWriter, this::newExampleElement, 1L + (i * messagesPerThread), messagesPerThread + (i * messagesPerThread)));
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }

    assertThat("Should have a total of " + (messagesPerThread * numThreads) + " messages", segment.getMetaData().getRecordCount(), is((int) (messagesPerThread * numThreads)));

  }

  @Test(timeout = 20000)
  public void testSegmentReaderCanHandleReadsByMultipleThreads() throws IOException, InterruptedException {
    final int numThreads = random.nextInt(50);
    final long messageCount = random.nextInt(10000);
    Thread[] threads = new Thread[numThreads];
    TestSegmentReader[] readers = new TestSegmentReader[numThreads];
    long recordsRead = 0;

    Segment<T> segment = openNewSegment(folder.newFolder("multi-thread-read"));
    Segment.Writer<T> writer = segment.getWriter();
    TestSegmentWriter<T> segmentWriter = new TestSegmentWriter<>(writer, this::newExampleElement, 1, messageCount);
    segmentWriter.run();


    for (int i = 0; i < numThreads; i++) {
      readers[i] = new TestSegmentReader<>(segment);
      threads[i] = new Thread(readers[i]);
      threads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    for (int i = 0; i < numThreads; i++) {
      recordsRead += readers[i].messageCount;
    }

    assertThat("Should have read " + messageCount + " messages from " + numThreads + " threads", recordsRead, is(messageCount * numThreads));

  }

}
