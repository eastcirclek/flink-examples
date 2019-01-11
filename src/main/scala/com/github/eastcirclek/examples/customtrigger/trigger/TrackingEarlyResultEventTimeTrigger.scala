package com.github.eastcirclek.examples.customtrigger.trigger

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.JavaConverters._

class TrackingEarlyResultEventTimeTrigger[T](eval: (T => Boolean)) extends Trigger[T, TimeWindow] {
  val timersDesc = new ListStateDescriptor[Long]("timers", classOf[Long])
  val countDesc = new ReducingStateDescriptor[Long]("count",
    new ReduceFunction[Long] { override def reduce(v1: Long, v2: Long): Long = v1+v2},
    classOf[Long]
  )
  val lastCountWhenFiringDesc = new ReducingStateDescriptor[Long]("lastCount",
    new ReduceFunction[Long] { override def reduce(v1: Long, v2: Long): Long = v1+v2},
    classOf[Long]
  )

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    ctx.getPartitionedState(countDesc).add(1)

    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      println(s"[onElement] $window $element - FIRE (allowed lateness)")
      fireOrContinue(ctx)
    } else {
      if (eval(element)) {
        println(s"[onElement] $window $element registerTimer_$timestamp for early fire")
        ctx.registerEventTimeTimer(timestamp)
        ctx.getPartitionedState(timersDesc).add(timestamp)
      }
      println(s"[onElement] $window $element registerTimer_${window.maxTimestamp}")
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time < window.maxTimestamp) {
      println(s"[onEventTime] $window time_$time - FIRE (registered event time)")
      println(s"[onEventTime] $window deleteTimer_$time")
      ctx.deleteEventTimeTimer(time)

      val timers = ctx.getPartitionedState(timersDesc)
      timers.update(timers.get.asScala.filter(_ != time).toSeq.asJava)

      fireOrContinue(ctx)
    } else if (time == window.maxTimestamp) {
      println(s"[onEventTime] $window time_$time - FIRE (maxtimestamp)")
      fireOrContinue(ctx)
    } else {
      println(s"[onEventTime] $window time_$time - CONTINUE")
      TriggerResult.CONTINUE
    }
  }

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(countDesc)
    ctx.mergePartitionedState(lastCountWhenFiringDesc)
    ctx.mergePartitionedState(timersDesc)

    val timers = ctx.getPartitionedState(timersDesc)
    if (timers.get != null) {
      timers.get.asScala.foreach { timestamp =>
        println(s"[onMerge] $window restoreTimer_$timestamp")
        ctx.registerEventTimeTimer(timestamp)
      }
    }

    println(s"[onMerge] $window registerTimer_${window.maxTimestamp}")
    ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  def fireOrContinue(ctx: Trigger.TriggerContext): TriggerResult = {
    val count_val = ctx.getPartitionedState(countDesc).get
    val lastCount = ctx.getPartitionedState(lastCountWhenFiringDesc)
    val lastCount_val = lastCount.get
    val diff = count_val - lastCount_val
    lastCount.add(diff)

    val result = if (diff > 0) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
    println(s"[fireOrContinue] count_${count_val} lastCount_${lastCount_val} $result")

    result
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    println(s"[clear] $window deleteTimer_${window.maxTimestamp}")
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def toString = "EarlyResultEventTimeTrigger()"
}
