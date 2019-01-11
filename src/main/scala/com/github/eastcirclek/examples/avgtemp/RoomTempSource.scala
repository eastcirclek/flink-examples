package com.github.eastcirclek.examples.avgtemp

import com.github.eastcirclek.examples.avgtemp.datatype.RoomTemperature
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

class RoomTempSource(meanF: Int, stdF: Int, stdF2: Int, numRooms: Int) extends SourceFunction[RoomTemperature]{
  @volatile var running = true

  override def run(ctx: SourceFunction.SourceContext[RoomTemperature]): Unit = {
    val generator = new RoomsTempGenerator(meanF, stdF, stdF2, numRooms)

    while(running) {
      val time = System.currentTimeMillis
      val results: Array[RoomTemperature] = generator.generate(time)
      results.foreach { result =>
        ctx.collectWithTimestamp(result, time)
      }
      ctx.emitWatermark(new Watermark(time))

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
