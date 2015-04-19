package com.chrisalbright;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static com.chrisalbright.LockUtil.withLock;

public class QueueEnvironment<T> {

  private final BlockingQueue<QueueFile<T>> queueFiles = Queues.newLinkedBlockingQueue();
  private final LinkedList<QueueFile<T>> preCommittedFiles = Lists.newLinkedList();
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;

  private final ReentrantLock takeLock = new ReentrantLock();
  private final ReentrantLock putLock = new ReentrantLock();

  private final Condition takeCondition = takeLock.newCondition();
  private final Condition putCondition = putLock.newCondition();

  public QueueEnvironment(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn) throws InterruptedException {
    this(rootPath, encoderFn, decoderFn, 100 * 1024);
  }

  public QueueEnvironment(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn, int maxFileSize) throws InterruptedException {
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

  private QueueFile<T> newQueueFile() {
    return null;
  }

  public int fileCount() {
    return queueFiles.size();
  }

  public void commit() {

  }

  public BlockingQueue<T> getQueue() throws InterruptedException {
    final QueueEnvironment<T> manager = this;

    if (queueFiles.size() == 0) {
      queueFiles.put(manager.newQueueFile());
    }

    return new BlockingQueue<T>() {
      @Override
      public boolean add(T t) {
        Preconditions.checkNotNull(t, "Null elements not accepted");
        try {
          while (!queueFiles.peek().push(t)) {
            queueFiles.put(manager.newQueueFile());
          }
          return true;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public boolean offer(T t) {
        return add(t);
      }

      @Override
      public void put(T t) throws InterruptedException {
        add(t);
      }

      @Override
      public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return add(t);
      }

      @Override
      public T take() throws InterruptedException {
        return withLock(takeLock, () -> {
          Optional<T> t;
          while (!(t = dequeue()).isPresent()) {
            takeCondition.await();
          }
          return t.get();
        });
      }

      @Override
      public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return withLock(takeLock, () -> {
          Optional<T> t;
          long nanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
          while (!(t = dequeue()).isPresent()) {
            if (nanos <= 0) return null;
            nanos = takeCondition.awaitNanos(nanos);
          }
          return t.get();
        });
      }

      @Override
      public T poll() {
        return dequeue().orElse(null);
      }

      @Override
      public T element() {
        return _peek().orElseThrow(() -> new RuntimeException("No elements to be taken"));
      }

      @Override
      public T peek() {
        return _peek().orElse(null);
      }

      private Optional<T> dequeue() {
        try {
          Optional<T> t;
          while (!(t = queueFiles.peek().fetch()).isPresent()) {
            queueFiles.put(manager.newQueueFile());
          }
          return t;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      private Optional<T> _peek() {
        try {
          Optional<T> t;
          while (!(t = queueFiles.peek().peek()).isPresent()) {
            queueFiles.put(manager.newQueueFile());
          }
          return t;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
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
      public int size() {
        Optional<Integer> count = StreamSupport.stream(queueFiles.spliterator(), false)
                                    .map(QueueFile::getRecordCount)
                                    .reduce((l, r) -> l + r);
        return count.orElse(0);
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }

      @Override
      public Iterator<T> iterator() {
        Iterator<Iterator<T>> queueFileIterators = StreamSupport.stream(queueFiles.spliterator(), false)
                                                     .map(new Function<QueueFile<T>, Iterator<T>>() {
                                                       @Override
                                                       public Iterator<T> apply(QueueFile<T> ts) {
                                                         return ts.iterator();
                                                       }
                                                     }).iterator();
        return Iterators.concat(queueFileIterators);
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
