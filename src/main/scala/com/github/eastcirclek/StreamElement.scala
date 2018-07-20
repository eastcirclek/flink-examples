package com.github.eastcirclek

trait StreamElement
case class MyWatermark(timestamp: Long) extends StreamElement
case class MyRecord(value: Char, timestamp: Long) extends StreamElement

