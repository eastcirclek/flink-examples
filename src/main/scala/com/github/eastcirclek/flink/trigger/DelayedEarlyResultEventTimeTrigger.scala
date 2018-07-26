package com.github.eastcirclek.flink.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class DelayedEarlyResultEventTimeTrigger[T](eval: (T => Boolean), delay: Long = 0) extends Trigger[T, TimeWindow] {
  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      TriggerResult.FIRE
    } else {
      if (eval(element)) {
        println(s"[onElement] $window - registerEventTimeTimer(${timestamp+delay} = ts_${timestamp} + delay_${delay})")
        ctx.registerEventTimeTimer(timestamp + delay)
      } else {
        println(s"[onElement] $window - registerEventTimeTimer(${window.maxTimestamp})")
        ctx.registerEventTimeTimer(window.maxTimestamp)
      }
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println(s"[onEventTime] $window $time")
    TriggerResult.FIRE_AND_PURGE
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

  override def toString = "DelayedEarlyResultEventTimeTrigger()"
}
