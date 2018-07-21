package com.github.eastcirclek

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time._
import org.apache.flink.util.Collector

object SessionWindow2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val records = Seq[StreamElement](
      MyRecord('a', 1),
      MyRecord('c', 7),
      MyWatermark(5),
      MyRecord('b', 4)
    )

    env
      .addSource( (context: SourceContext[MyRecord]) =>
        records foreach {
          case MyWatermark(timestamp) =>
            println(s"Generate a watermark @ $timestamp")
            context.emitWatermark(new Watermark(timestamp))
            Thread.sleep(100)
          case record@MyRecord(value, timestamp) =>
            println(s"$value @ $timestamp")
            context.collectWithTimestamp(record, timestamp)
            Thread.sleep(100)
        }
      )
      .windowAll(EventTimeSessionWindows.withGap(milliseconds(3)))
      .apply(
        (window, iterator, collector: Collector[String]) =>
          collector.collect(window.toString + " : " + iterator.mkString(", "))
      )
      .print()

    env.execute()
  }
}
