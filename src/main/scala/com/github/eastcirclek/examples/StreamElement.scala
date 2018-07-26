package com.github.eastcirclek.examples

trait StreamElement
case class MyWatermark(timestamp: Long) extends StreamElement
case class MyRecord(value: Char, timestamp: Long, last: Boolean = false) extends StreamElement {
  override def toString: String =
    if (!last) {
      s"MyRecord($value,$timestamp)"
    } else {
      s"MyRecord($value,$timestamp,last=true)"
    }
}
