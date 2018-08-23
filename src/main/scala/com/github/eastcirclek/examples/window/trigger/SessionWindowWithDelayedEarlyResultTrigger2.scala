package com.github.eastcirclek.examples.window.trigger

import com.github.eastcirclek.examples.{MyRecord, MyWatermark, StreamElement}
import com.github.eastcirclek.flink.trigger.DelayedEarlyResultEventTimeTrigger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time._
import org.apache.flink.util.Collector

object SessionWindowWithDelayedEarlyResultTrigger2 {
  def main(args: Array[String]): Unit = {
    val records = Seq[StreamElement](
      MyRecord('a', 2),
      MyRecord('b', 4),
      MyRecord('c', 6, true),
      MyRecord('d', 8),
      MyRecord('e', 10),
      MyRecord('f', 12, true),
      MyRecord('g', 14),
      MyRecord('h', 16),
      MyRecord('i', 18, true)
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
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(Int, MyRecord)](milliseconds(2)) {
          override def extractTimestamp(tp: (Int, MyRecord)): Long = {
            val element = tp._2
            element.timestamp
          }
        }
      )
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(milliseconds(5)))
      .trigger(new DelayedEarlyResultEventTimeTrigger[(Int, MyRecord)](_._2.last))
      .apply(
        (key, window, iterator, collector: Collector[String]) =>
          collector.collect(window.toString + " : " + iterator.map(_._2).mkString(", "))
      )
      .print()

    env.execute()
  }
}
