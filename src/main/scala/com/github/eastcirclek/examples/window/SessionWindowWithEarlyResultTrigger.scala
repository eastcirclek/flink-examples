package com.github.eastcirclek.examples.window

import com.github.eastcirclek.examples.{MyRecord, MyWatermark, StreamElement}
import com.github.eastcirclek.flink.trigger.EarlyResultEventTimeTrigger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time._
import org.apache.flink.util.Collector

object SessionWindowWithEarlyResultTrigger {
  def main(args: Array[String]): Unit = {
    val records = Seq[StreamElement](
      MyRecord('a', 2),
      MyRecord('b', 4),
      MyRecord('c', 6, true),
      MyRecord('g', 14),
      MyRecord('h', 16),
      MyRecord('i', 18, true),
      MyRecord('d', 8),
      MyRecord('e', 10),
      MyRecord('f', 12, true),
      MyWatermark(12),
      MyWatermark(18),
      MyWatermark(22),
      MyRecord('j', 30),
      MyRecord('l', 34, true),
      MyRecord('k', 32),
      MyWatermark(34),
      MyRecord('m', 35),
      MyWatermark(42)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(50)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env
      .addSource( (context: SourceContext[MyRecord]) =>
        records foreach {
          case MyWatermark(timestamp) =>
            println(s"Generate a watermark @ $timestamp")
            context.emitWatermark(new Watermark(timestamp))
            Thread.sleep(200)
          case record@MyRecord(value, timestamp, isLast) =>
            if (isLast) {
              println(s"$value @ $timestamp last")
            } else {
              println(s"$value @ $timestamp")
            }
            context.collectWithTimestamp(record, timestamp)
            Thread.sleep(200)
        }
      )
      .map(rec => (1, rec))
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(milliseconds(5)))
      .trigger(new EarlyResultEventTimeTrigger[(Int, MyRecord)](_._2.last))
      .apply(
        (key, window, iterator, collector: Collector[String]) =>
          collector.collect("[apply] " + window.toString + " : " + iterator.map(_._2).mkString(", "))
      )
      .print()

    env.execute()
  }
}
