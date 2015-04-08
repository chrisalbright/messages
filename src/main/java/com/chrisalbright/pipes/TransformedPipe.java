package com.chrisalbright.pipes;

import com.chrisalbright.functions.Fn;

public class TransformedPipe<Input, Output> implements Out<Output> {
  private final Pipe<Input, Input> pipe;
  private final Fn<Input, Output> fn;

  public TransformedPipe(Pipe<Input, Input> pipe, Fn<Input, Output> fn) {
    this.pipe = pipe;
    this.fn = fn;
  }

  @Override
  public Output out() {
    return fn.apply(pipe.out());
  }
}
