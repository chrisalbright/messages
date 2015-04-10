package com.chrisalbright;

import com.google.common.base.Charsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BufferTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testCharBufferStrings() throws IOException {
    String s = "Hello World";
    char[] c = s.toCharArray();
    byte[] bytes1 = s.getBytes();
    byte[] bytes2 = s.getBytes(Charsets.UTF_8);
    byte[] bytes3 = s.getBytes(Charsets.UTF_16);

    RandomAccessFile file1 = new RandomAccessFile(folder.newFile("buffer1"), "rw");
    RandomAccessFile file2 = new RandomAccessFile(folder.newFile("buffer2"), "rw");
    RandomAccessFile file3 = new RandomAccessFile(folder.newFile("buffer3"), "rw");
    RandomAccessFile file4 = new RandomAccessFile(folder.newFile("buffer4"), "rw");
    RandomAccessFile file5 = new RandomAccessFile(folder.newFile("buffer5"), "rw");

    FileChannel channel1 = file1.getChannel();
    FileChannel channel2 = file2.getChannel();
    FileChannel channel3 = file3.getChannel();
    FileChannel channel4 = file4.getChannel();
    FileChannel channel5 = file5.getChannel();

    MappedByteBuffer buffer1 = channel1.map(FileChannel.MapMode.READ_WRITE, 0, 22);
    MappedByteBuffer buffer2 = channel2.map(FileChannel.MapMode.READ_WRITE, 0, 22);
    MappedByteBuffer buffer3 = channel3.map(FileChannel.MapMode.READ_WRITE, 0, 22);
    MappedByteBuffer buffer4 = channel4.map(FileChannel.MapMode.READ_WRITE, 0, 22);
    MappedByteBuffer buffer5 = channel5.map(FileChannel.MapMode.READ_WRITE, 0, 32);

    buffer1.asCharBuffer().put(s);
    buffer2.asCharBuffer().put(c);
    buffer3.put(bytes1);
    buffer4.put(bytes2);
    buffer5.put(bytes3);

    System.out.println(s);

  }
}
