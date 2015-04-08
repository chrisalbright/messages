package com.chrisalbright.pipes;

import com.chrisalbright.functions.FilterFn;

public class FilteredPipe<Input> implements Out<Input> {

  final Pipe<Input, Input> pipe;
  final FilterFn<Input> fn;

  public FilteredPipe(Pipe<Input, Input> pipe, FilterFn<Input> fn) {
    this.pipe = pipe;
    this.fn = fn;
  }

  @Override
  public Input out() {
    Input input = null;
    while ((input = pipe.out()) != null) {
      if (fn.apply(input)) {
        return input;
      }
    }
    return null;
  }
}
