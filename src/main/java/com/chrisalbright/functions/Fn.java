package com.chrisalbright.functions;

public interface Fn<F, T> {
  public T apply(F from);
}
