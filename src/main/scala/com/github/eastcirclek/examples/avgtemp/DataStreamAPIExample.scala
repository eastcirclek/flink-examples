package com.github.eastcirclek.examples.avgtemp

import com.github.eastcirclek.examples.avgtemp.datatype.RoomTemperature
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object DataStreamAPIExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData: DataStream[RoomTemperature] = env.addSource(
      new RoomTempSource(meanF=70, stdF=10, stdF2=5, numRooms=5))

    sensorData
      .keyBy(roomTemp => roomTemp.location)
      .timeWindow(Time.seconds(3))
      .apply { (location: String,
                window: TimeWindow,
                iter: Iterable[RoomTemperature],
                collector: Collector[RoomTemperature]) =>
        val cnt = iter.size
        val sum = iter.map(_.temp).sum
        val avg = sum/cnt
        collector.collect(
          RoomTemperature(location, (avg-32)*0.556, window.getEnd)
        )
      }
      .print()

    env.execute()
  }
}
