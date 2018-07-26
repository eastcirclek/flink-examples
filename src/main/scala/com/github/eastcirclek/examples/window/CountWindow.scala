package com.github.eastcirclek.examples.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object CountWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env
      .fromElements(1 to 6:_*)
      .countWindowAll(3, 2)
      .apply {
        (_, iterator, collector: Collector[String]) =>
          collector.collect(iterator.mkString(","))
      }
      .print()

    env.execute()
  }
}