package com.github.eastcirclek

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time._
import org.apache.flink.util.Collector

object SlidingEventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // - Records, a@4, b@7, and c@12, are arriving out-of-order
    // - TimeWindow{start=-5, end=5} is not going to be created
    //     when 'a' arrives because the initial watermark is 5
    val records = Seq(
      MyWatermark(5),
      MyRecord('b', 7),
      MyRecord('a', 4),
      MyWatermark(10),
      MyRecord('c', 12),
      MyWatermark(15),
      MyWatermark(20)
    )

    env
      .addSource( (context: SourceContext[Char]) =>
        records foreach {
          case MyWatermark(timestamp) =>
            context.emitWatermark(new Watermark(timestamp))
          case MyRecord(value, timestamp) =>
            context.collectWithTimestamp(value, timestamp)
        }
      )
      .timeWindowAll(milliseconds(10), milliseconds(5))
      .apply(
        (window, iterator, collector: Collector[String]) =>
          collector.collect(window.toString + " : " + iterator.mkString(", "))
      )
      .print()

    env.execute()
  }
}
