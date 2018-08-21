package com.github.eastcirclek.flink.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TrackingEventTimeTrigger[T] extends Trigger[T, TimeWindow] {
  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      println(s"[onElement] $window - FIRE (allowed lateness)")
      TriggerResult.FIRE
    } else {
      println(s"[onElement] $window registerTimer_${window.maxTimestamp} - CONTINUE")
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) {
      println(s"[onEventTime] $window time_$time - FIRE (event time)")
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
    println(s"[clear] $window - deleteTimer_${window.maxTimestamp}")
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"[onMerge] $window - registerTimer_${window.maxTimestamp}")
    ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  override def toString = "EventTimeTrigger()"
}
