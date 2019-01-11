package com.github.eastcirclek.examples.avgtemp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SQLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData = env.addSource(
      new RoomTempSource(meanF=70, stdF=10, stdF2=5, numRooms=5))

    // register DataStream
    tableEnv.registerDataStream(
      "sensorData", sensorData, 'location, 'temp.as('tempF), 'rowtime.rowtime)

    // query registered Table
    val avgTempCTable: Table = tableEnv
      .sqlQuery("""
             SELECT TUMBLE_START(rowtime, INTERVAL '3' SECOND) as wStart,
                 location,
                 (avg(tempF)-32)*0.556 as avgTempC
             FROM sensorData
             GROUP BY location, TUMBLE(rowtime, INTERVAL '3' SECOND)
                """)

    tableEnv.toAppendStream[Row](avgTempCTable).print()
    env.execute()
  }
}
