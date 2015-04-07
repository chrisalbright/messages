package com.chrisalbright;

import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.Long;import java.lang.String;import java.lang.System;import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class QueueFileTest {

  QueueFile<byte[]> q;
  public static final Charset CHARSET = Charset.forName("UTF-8");

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    RandomAccessFile raf = new RandomAccessFile(folder.newFile("queue-file"), "rw");
    q = new QueueFile<byte[]>(raf, Converters.BYTE_ARRAY_CONVERTER);
  }

  @Test
  public void testAddSingleItemToQueueFile() throws IOException {
    String expected = "hello world";
    q.push(expected.getBytes(CHARSET));

    Optional<byte[]> optional = q.fetch();
    assertTrue(optional.isPresent());
    byte[] data = optional.get();

    String actual = new String(data);
    assertThat(actual, is(expected));
  }

  @Test
  public void testAddMultipleItemsToQueueFile() throws IOException {
    String expected1 = "hello world";
    String expected2 = "hello dolly";
    String expected3 = "howdee doodie";

    q.push(expected1.getBytes(CHARSET));
    q.push(expected2.getBytes(CHARSET));
    q.push(expected3.getBytes(CHARSET));

    byte[] data1 = q.fetch().get();
    byte[] data2 = q.fetch().get();
    byte[] data3 = q.fetch().get();

    String actual1 = new String(data1);
    String actual2 = new String(data2);
    String actual3 = new String(data3);

    assertThat(actual1, is(expected1));
    assertThat(actual2, is(expected2));
    assertThat(actual3, is(expected3));
  }

  @Test
  public void testFetchOnEmptyQueueFileReturnsEmptyOption() throws IOException {
    assertEquals(q.fetch(), Optional.empty());
  }

  @Test
  public void testAddAnyTypeToQueueFile() throws IOException {
    QueueFile<Long> q = new QueueFile<Long>(new RandomAccessFile(folder.newFile(), "rw"), Converters.LONG_CONVERTER);

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

  @Test
  public void testPerformance() throws IOException {
    SecureRandom r = new SecureRandom();
    int messages = 10000;
    byte[][] data = new byte[messages][1024];
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
    w = Stopwatch.createStarted();
    while ((val = q.fetch()) != Optional.<byte[]>empty()){}
    w.stop();
    System.out.println("Read " + messages + " messages in " + w.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");

  }
}
