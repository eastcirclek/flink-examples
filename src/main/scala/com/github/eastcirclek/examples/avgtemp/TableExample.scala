package com.github.eastcirclek.examples.avgtemp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData = env.addSource(
      new RoomTempSource(meanF=70, stdF=10, stdF2=5, numRooms=5))

    // convert DataSet into Table
    val sensorTable: Table = sensorData
      .toTable(tableEnv, 'location, 'temp.as('tempF), 'rowtime.rowtime)

    // define query on Table
    val avgTempCTable: Table = sensorTable
      .window(Tumble over 3.seconds on 'rowtime as 'w)
      .groupBy('location, 'w)
      .select(
        'w.start as 'wStart,
        'location,
        (('tempF.avg - 32) * 0.556) as 'avgTempC
      )

    tableEnv.toAppendStream[Row](avgTempCTable).print()
    env.execute()
  }
}
