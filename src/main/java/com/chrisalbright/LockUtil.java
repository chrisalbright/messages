package com.chrisalbright;

import java.util.concurrent.locks.Lock;

public class LockUtil {
  public static <T> T withLock(Lock lock, LockedOp<T> lockedOp) throws LockedOpException {
    lock.lock();
    try {
      return lockedOp.apply();
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
  public interface LockedOp<T> {
    public T apply() throws Throwable;
  }
}
