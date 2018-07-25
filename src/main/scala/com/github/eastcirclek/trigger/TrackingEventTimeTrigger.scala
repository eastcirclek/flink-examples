package com.github.eastcirclek.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TrackingEventTimeTrigger[T] extends Trigger[T, TimeWindow] {
  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      TriggerResult.FIRE
    } else {
      println(s"[onElement] $window - registerEventTimeTimer(${window.maxTimestamp})")
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println(s"[onEventTime] $window $time")
    if (time == window.maxTimestamp) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    println(s"[clear] $window - deleteEventTimeTimer(${window.maxTimestamp})")
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    println(s"[onMerge] $window - registerEventTimeTimer(${window.maxTimestamp})")
    ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  override def toString = "EventTimeTrigger()"
}
