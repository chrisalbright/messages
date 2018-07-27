package com.chrisalbright;

import java.util.concurrent.locks.Lock;

public class LockUtil {
  public static <T> T withLock(Lock lock, LockedOp<T> lockedOp) throws LockedOpException {
    lock.lock();
    try {
      return lockedOp.run();
    } catch (Throwable t) {
      throw new LockedOpException(t);
    } finally {
      lock.unlock();
    }
  }
  public static void withLock(Lock lock, LockedBlock block) throws LockedOpException {
    lock.lock();
    try {
      block.run();
    } catch (Throwable t) {
      throw new LockedOpException(t);
    } finally {
      lock.unlock();
    }
  }

  public static class LockedOpException extends RuntimeException {
    public LockedOpException(Throwable t) {
      super(t);
    }
  }

  @FunctionalInterface
  public interface LockedOp<T> {
    T run() throws Throwable;
  }

  @FunctionalInterface
  public interface LockedBlock {
    void run() throws Throwable;
  }
}
