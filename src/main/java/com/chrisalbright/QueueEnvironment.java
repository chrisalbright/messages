package com.chrisalbright;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.io.Files;
import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.chrisalbright.LockUtil.withLock;
import static com.google.common.primitives.UnsignedLong.ONE;

public class QueueEnvironment<T> {

  private final BlockingDeque<QueueFile<T>> activeFiles = Queues.newLinkedBlockingDeque();
  private final LinkedList<QueueFile<T>> consumedFiles = Lists.newLinkedList();
  private final Function<T, byte[]> encoderFn;
  private final Function<byte[], T> decoderFn;

  private final ExecutorService deleteExecutor;
  private final ReentrantLock takeLock = new ReentrantLock();
  private final ReentrantLock putLock = new ReentrantLock();
  private final ReentrantLock queueFileLock = new ReentrantLock();

  private final Condition takeCondition = takeLock.newCondition();
  private final Condition putCondition = putLock.newCondition();

  private final File rootPath;
  private final int maxFileSize;

  public QueueEnvironment(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn) throws InterruptedException {
    this(rootPath, encoderFn, decoderFn, 100 * 1024);
  }

  public QueueEnvironment(File rootPath, Function<T, byte[]> encoderFn, Function<byte[], T> decoderFn, int maxFileSize) throws InterruptedException {
    this.encoderFn = encoderFn;
    this.decoderFn = decoderFn;
    this.rootPath = rootPath;
    this.maxFileSize = maxFileSize;
    this.deleteExecutor = Executors.newSingleThreadExecutor();
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
        .forEach(activeFiles::addLast);
    }
  }

  private QueueFile<T> newQueueFile() {
    return LockUtil.withLock(queueFileLock, () -> {
      QueueFile<T> lastQueueFile = activeFiles.peekLast();
      String lastFileNo = Files.getNameWithoutExtension(lastQueueFile.getFileName());
      String nextFileNo = UnsignedLong.valueOf(lastFileNo).plus(ONE).toString();
      return newQueueFile(nextFileNo);
    });
  }

  private QueueFile<T> newQueueFile(String id) throws IOException {
    Stream<Integer> zeros = Stream.iterate(0, (n) -> 0);
    StringBuilder fileName = new StringBuilder("");
    zeros.limit(20 - id.length()).forEach(fileName::append);
    fileName.append(id).append(".queuefile");
    File queueFile = new File(rootPath, fileName.toString());
    return new QueueFile<>(queueFile, encoderFn, decoderFn, maxFileSize);
  }

  public int fileCount() {
    return activeFiles.size() + consumedFiles.size();
  }

  public void commit() {
    withLock(queueFileLock, () -> {
      QueueFile<T> q;
      while ((q = consumedFiles.poll()) != null) {
        q.commit();
        cleanFile(q);
      }
      activeFiles.peekLast().commit();
      return Void.TYPE;
    });
  }

  private boolean cleanFile(final QueueFile<T> q) {
    return new File(rootPath, q.getFileName()).delete();
  }

  public void close() throws IOException{
    withLock(queueFileLock, () -> {
      for (QueueFile<T> activeFile : activeFiles) {
        activeFile.close();
      }
      for (QueueFile<T> consumedFile : consumedFiles) {
        consumedFile.close();
      }
      return Void.TYPE;
    });
  }

  public BlockingQueue<T> getQueue() throws InterruptedException, IOException {
    final QueueEnvironment<T> manager = this;

    if (activeFiles.size() == 0) {
      activeFiles.put(manager.newQueueFile("1"));
    }

    return new BlockingQueue<T>() {
      @Override
      public boolean add(T t) {
        Preconditions.checkNotNull(t, "Null elements not accepted");
        try {
          while (!activeFiles.peekLast().push(t)) {
            activeFiles.putLast(manager.newQueueFile());
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
          while (!(t = activeFiles.peekFirst().fetch()).isPresent()) {
            consumedFiles.addLast(activeFiles.takeFirst());
          }
          return t;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      private Optional<T> _peek() {
        try {
          Optional<T> t;
          while (!(t = activeFiles.peekFirst().peek()).isPresent()) {
            activeFiles.putFirst(manager.newQueueFile());
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
        Optional<Integer> count = StreamSupport.stream(activeFiles.spliterator(), false)
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
        Iterator<Iterator<T>> queueFileIterators = StreamSupport.stream(activeFiles.spliterator(), false)
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
