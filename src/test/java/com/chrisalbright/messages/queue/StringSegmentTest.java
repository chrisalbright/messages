package com.chrisalbright.messages.queue;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class StringSegmentTest extends SegmentTest<String> {
  AtomicInteger exampleElementIndex = new AtomicInteger(0);
  String[] exampleElements = {
      "Don't cry because it's over, smile because it happened.",
      "Be yourself; everyone else is already taken.",
      "Two things are infinite: the universe and human stupidity; and I'm not sure about the universe.",
      "So many books, so little time.",
      "Be who you are and say what you feel, because those who mind don't matter, and those who matter don't mind.",
      "You only live once, but if you do it right, once is enough.",
      "If you tell the truth, you don't have to remember anything.",
      "A friend is someone who knows all about you and still loves you.",
      "Always forgive your enemies; nothing annoys them so much.",
      "To live is the rarest thing in the world. Most people exist, that is all.",
      "I am so clever that sometimes I don't understand a single word of what I am saying."
  };

  @Override
  String newExampleElement() {
    int index = exampleElementIndex.getAndIncrement();
    return exampleElements[index % exampleElements.length];
  }
}
