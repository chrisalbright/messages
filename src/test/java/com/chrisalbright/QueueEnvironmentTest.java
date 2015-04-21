package com.chrisalbright;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueueEnvironmentTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testQueueCreatesFiles() throws InterruptedException, IOException {

    QueueEnvironment<String> env = new QueueEnvironment<>(folder.newFolder(), Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);
    BlockingQueue<String> queue = env.getQueue();

    String message = "The quick brown fox jumps over the lazy dog.";

    int messageCount = 100;

    int dataLength = 1024 - QueueFile.Header.LENGTH;
    int messageLength = message.length() + Integer.BYTES;
    int messagesPerFile = dataLength / messageLength;
    int expectedFiles = Double.valueOf(Math.ceil(1.0 * messageCount / messagesPerFile)).intValue();

    for (int i = 0; i < messageCount; i++) {
      queue.add(message);
    }

    assertThat(queue.size(), is(messageCount));
    assertThat(env.fileCount(), is(expectedFiles));
  }

  @Test
  public void testReadFromQueue() throws IOException, InterruptedException {
    File rootPath = folder.newFolder();
    QueueEnvironment<String> env = new QueueEnvironment<>(rootPath, Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);
    BlockingQueue<String> queue = env.getQueue();

    String message = "%d - The quick brown fox jumps over the lazy dog.";

    int messageCount = 10;

    for (int i = 0; i < messageCount; i++) {
      queue.add(String.format(message, i));
    }


    for (int i = 0; i < messageCount; i++) {
      assertThat(queue.take(), is(String.format(message, i)));
    }

    env.close();
    env = new QueueEnvironment<>(rootPath, Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);

    queue = env.getQueue();
    for (int i = 0; i < messageCount; i++) {
      assertThat(queue.take(), is(String.format(message, i)));
    }

  }

  @Test
  public void testCommitQueue() throws IOException, InterruptedException {
    File rootPath = folder.newFolder();
    QueueEnvironment<String> env = new QueueEnvironment<>(rootPath, Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);
    BlockingQueue<String> queue = env.getQueue();

    String message = "%d - The quick brown fox jumps over the lazy dog.";

    int messageCount = 10;

    for (int i = 0; i < messageCount; i++) {
      queue.add(String.format(message, i));
    }


    for (int i = 0; i < messageCount; i++) {
      assertThat(queue.take(), is(String.format(message, i)));
    }

    System.out.println(queue.size());
    env.commit();
    env.close();

    env = new QueueEnvironment<>(rootPath, Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);
    queue = env.getQueue();

    assertThat(queue.size(), is(0));

  }

  @Test
  public void testCleansFiles() throws IOException, InterruptedException {

    QueueEnvironment<String> env = new QueueEnvironment<>(folder.newFolder(), Converters.STRING_ENCODER, Converters.STRING_DECODER, 1024);
    BlockingQueue<String> queue = env.getQueue();

    String message = "The quick brown fox jumps over the lazy dog.";

    int messageCount = 100;

    for (int i = 0; i < messageCount; i++) {
      queue.add(message);
    }

    assertThat(env.fileCount(), greaterThan(1));

    for (int i = 0; i < messageCount; i++) {
      queue.take();
    }

    env.commit();

    assertThat(env.fileCount(), is(1));
  }
}
