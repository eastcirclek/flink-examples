package com.github.eastcirclek.examples.avgtemp

import com.github.eastcirclek.examples.avgtemp.datatype.RoomTemperature

import scala.util.Random

class RoomTempGenerator(val roomId: String, val mean: Double, val std: Double) {
  val rand = new Random()
  def generate(time: Long): RoomTemperature =
    RoomTemperature(
      location = roomId,
      temp   = rand.nextGaussian()*std+mean,
      time   = time
    )
}

class RoomsTempGenerator(mean: Double, std: Double, std2: Double, numRooms: Int) {
  val rand1 = new Random()
  val rand2 = new Random()

  private val generators: Array[RoomTempGenerator] =
    Array.tabulate(numRooms){ idx =>
      new RoomTempGenerator(
        roomId = s"room-$idx",
        mean   = rand1.nextGaussian()*std + mean,
        std    = rand2.nextGaussian()*std2
      )
    }

  def generate(time: Long): Array[RoomTemperature] =
    generators.map(_.generate(time))
}
