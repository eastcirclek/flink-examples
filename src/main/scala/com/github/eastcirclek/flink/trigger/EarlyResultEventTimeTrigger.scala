package com.github.eastcirclek.flink.trigger

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ListStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.JavaConverters._

class EarlyResultEventTimeTrigger[T](eval: (T => Boolean)) extends Trigger[T, TimeWindow] {
  val timersDesc = new ListStateDescriptor[Long]("timers", classOf[Long])
  val countDesc = new AggregatingStateDescriptor("count", LongAdder.create(), classOf[Long])
  val lastCountWhenFiringDesc = new AggregatingStateDescriptor("lastCount", LongAdder.create(), classOf[Long])

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    ctx.getPartitionedState(countDesc).add(1)

    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      fireOrContinue(ctx)
    } else {
      if (eval(element)) {
        val timers = ctx.getPartitionedState(timersDesc)
        timers.add(timestamp)
        ctx.registerEventTimeTimer(timestamp)
      }
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time < window.maxTimestamp) {
      ctx.deleteEventTimeTimer(time)

      val timers = ctx.getPartitionedState(timersDesc)
      timers.update(timers.get.asScala.filter(_ != time).toSeq.asJava)

      fireOrContinue(ctx)
    } else if (time == window.maxTimestamp) {
      fireOrContinue(ctx)
    } else {
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
        ctx.registerEventTimeTimer(timestamp)
      }
    }

    ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  def fireOrContinue(ctx: Trigger.TriggerContext): TriggerResult = {
    val count_val = ctx.getPartitionedState(countDesc).get
    val lastCount = ctx.getPartitionedState(lastCountWhenFiringDesc)
    val lastCount_val = lastCount.get
    val diff = count_val - lastCount_val
    lastCount.add(diff)

    if (diff > 0) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge: Boolean = true

  override def toString = "EarlyResultEventTimeTrigger()"
}

object LongAdder {
  def create(): AggregateFunction[Long, Long, Long] = {
    new AggregateFunction[Long, Long, Long] {
      override def add(n: Long, acc: Long): Long = n + acc
      override def createAccumulator(): Long = 0
      override def getResult(acc: Long): Long = acc
      override def merge(n1: Long, n2: Long): Long = n1 + n2
    }
  }
}
