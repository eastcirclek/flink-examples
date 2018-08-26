package com.github.eastcirclek.flink.function

import org.apache.flink.api.common.functions.AggregateFunction

object LongAdder {
  def create(): AggregateFunction[Long, Long, Long] = {
    new AggregateFunction[Long, Long, Long] {
      override def add(n: Long, acc: Long): Long = n + acc
      override def createAccumulator(): Long = 0
      override def getResult(acc: Long): Long = acc
      override def merge(n1: Long, n2: Long): Long = n1 + n2
    }
  }
}
