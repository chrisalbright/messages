package com.chrisalbright;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class QueueManager<T> {

  private final List<QueueFile<T>> queueFiles = new LinkedList<>();
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;

  public QueueManager(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn) {
    this(rootPath, encoderFn, decoderFn, 100 * 1024);
  }

  public QueueManager(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn, int maxFileSize) {
    this.encoderFn = encoderFn;
    this.decoderFn = decoderFn;
    if (!rootPath.exists()) {
      if (!rootPath.mkdirs()) {
        throw new IllegalStateException("Unable to create directory at " + rootPath.getAbsolutePath());
      }
    } else {

      Arrays.stream(rootPath.listFiles((File f) -> (f.canRead() && f.canWrite() && f.isFile() && Files.getFileExtension(f.getAbsolutePath()).equals("queuefile"))))
        .sorted()
        .map((File f) -> {
          try {
            return new QueueFile<T>(f, encoderFn, decoderFn, maxFileSize);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        })
        .forEach(queueFiles::add);
    }
  }

  public int fileCount() {
    return queueFiles.size();
  }

  public void commit() {

  }

  public BlockingQueue<T> getQueue() {
    return new BlockingQueue<T>() {
      @Override
      public boolean add(T t) {
        Preconditions.checkNotNull(t, "Null elements not accepted");

        return false;
      }

      @Override
      public boolean offer(T t) {
        return false;
      }

      @Override
      public void put(T t) throws InterruptedException {

      }

      @Override
      public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
      }

      @Override
      public T take() throws InterruptedException {
        return null;
      }

      @Override
      public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
      }

      @Override
      public int remainingCapacity() {
        return Integer.MAX_VALUE;
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public int drainTo(Collection<? super T> c) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
      }

      @Override
      public T remove() {
        throw new UnsupportedOperationException("Remove is not supported in this context.");
      }

      @Override
      public T poll() {
        return null;
      }

      @Override
      public T element() {
        return null;
      }

      @Override
      public T peek() {
        return null;
      }

      @Override
      public int size() {
        // loop over list of queue files, and sum record count
        return 0;
      }

      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public Iterator<T> iterator() {
        // iterator of iterators
        return null;
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public <T1> T1[] toArray(T1[] a) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public boolean addAll(Collection<? extends T> c) {
        return false;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException("How do you expect this to work?");
      }
    };
  }
}
