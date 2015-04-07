package com.chrisalbright;

import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

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

  private QueueFile<String> openStringQueueFile() throws FileNotFoundException {
    return new QueueFile<String>(f, Converters.STRING_CONVERTER);
  }

  @Test
  public void testAddSingleItemToQueueFile() throws IOException {
    String expected = "hello world";
    q.push(expected);

    Optional<String> optional = q.fetch();
    assertTrue(optional.isPresent());

    String actual = optional.get();
    assertThat(actual, is(expected));
  }

  @Test
  public void testAddMultipleItemsToQueueFile() throws IOException {
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
  public void testFetchOnEmptyQueueFileReturnsEmptyOption() throws IOException {
    assertEquals(q.fetch(), Optional.empty());
  }

  @Test
  public void testCRC() {
    CRC32 crc32a = new CRC32();
    CRC32 crc32b = new CRC32();

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

    crc32a.update(buffer1);
    crc32b.update(buffer2);

    Long crc1 = crc32a.getValue();
    Long crc2 = crc32b.getValue();

    assertEquals(crc1, crc2);
  }

  @Test
  public void testAddAnyTypeToQueueFile() throws IOException {
    QueueFile<Long> q = new QueueFile<Long>(folder.newFile(), Converters.LONG_CONVERTER);

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

  @Ignore
  @Test
  public void testPerformance() throws IOException {
    SecureRandom r = new SecureRandom();
    int messages = 100000;
    int messageSize = 1024;
    File f = folder.newFile();
    QueueFile<byte[]> q = new QueueFile<byte[]>(f, Converters.BYTE_ARRAY_CONVERTER, 100 * 1024 * 1024);
    byte[][] data = new byte[messages][messageSize];
    Stopwatch w = Stopwatch.createStarted();
    for (int i = 0; i < messages; i++) {
      r.nextBytes(data[i]);
    }
    w.stop();
    System.out.println("Generated " + messages + " random messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    w = Stopwatch.createStarted();
    for (int i = 0; i < messages; i++) {
      q.push(data[i]);
    }
    w.stop();
    System.out.println("Wrote " + messages + " random messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    Optional<byte[]> val = Optional.empty();
    byte[] bytes = new byte[messageSize];
    w = Stopwatch.createStarted();
    while ((val = q.fetch()) != Optional.<byte[]>empty()) {
      bytes = val.get();
    }
    w.stop();
    System.out.println("Read " + messages + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

    System.out.println("File size: " + f.length() / 1024 / 1024);

  }

  @Test
  public void testDoesNotExceedMaxFilesize() throws IOException {
    int messages = 99;
    int messageSize = 1024;

    File f = folder.newFile();
    QueueFile<byte[]> q = new QueueFile<byte[]>(f, Converters.BYTE_ARRAY_CONVERTER, 100 * 1024);

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
  public void testSavesReadPosition() throws IOException {
    q.push("Hello World");
    q.push("Hello Dolly");

    String first = q.fetch().get();

    q.commit();
    q.close();

    q = openStringQueueFile();

    String second = q.fetch().get();

    assertThat(second, is("Hello Dolly"));

  }

}
