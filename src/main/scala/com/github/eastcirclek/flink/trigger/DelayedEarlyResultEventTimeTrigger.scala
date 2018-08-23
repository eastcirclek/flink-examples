package com.github.eastcirclek.flink.trigger

import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.JavaConverters._

class DelayedEarlyResultEventTimeTrigger[T](eval: (T => Boolean)) extends Trigger[T, TimeWindow] {
  val timersAcrossMerges = new ListStateDescriptor[Long]("keptTimers", createTypeInformation[Long])

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      println(s"[onElement] $window $element - FIRE (allowed lateness)")
      TriggerResult.FIRE
    } else {
      if (eval(element)) {
        val timers = ctx.getPartitionedState(timersAcrossMerges)
        timers.add(timestamp)
        println(s"[onElement] $window $element registerTimer_$timestamp for early fire")
        ctx.registerEventTimeTimer(timestamp)
      }
      println(s"[onElement] $window $element registerTimer_${window.maxTimestamp} - CONTINUE")
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) {
      println(s"[onEventTime] $window time_$time - FIRE (maxtimestamp)")
      TriggerResult.FIRE
    } else if (time < window.maxTimestamp) {
      println(s"[onEventTime] $window time_$time - FIRE (registered event time)")
      println(s"[onEventTime] $window deleteTimer_$time")
      ctx.deleteEventTimeTimer(time)

      val timers = ctx.getPartitionedState(timersAcrossMerges)
      timers.update(timers.get.asScala.filter(_ != time).toSeq.asJava)

      TriggerResult.FIRE
    } else {
      println(s"[onEventTime] $window time_$time - CONTINUE")
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    println(s"[clear] $window deleteTimer_${window.maxTimestamp}")
    val timers = ctx.getPartitionedState(timersAcrossMerges)
    if (timers.get != null) {
      timers.get.asScala.foreach { timestamp =>
        println(s"[clear] $window deleteTimer_$timestamp")
        ctx.deleteEventTimeTimer(timestamp)
      }
    }
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(timersAcrossMerges)

    val timers = ctx.getPartitionedState(timersAcrossMerges)
    if (timers.get != null) {
      timers.get.asScala.foreach { timestamp =>
        println(s"[onMerge] $window restoreTimer_$timestamp")
        ctx.registerEventTimeTimer(timestamp)
      }
    }

    println(s"[onMerge] $window registerTimer_${window.maxTimestamp}")
    ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  override def toString = "DelayedEarlyResultEventTimeTrigger()"
}
