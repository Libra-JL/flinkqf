package com.qianfeng.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

/**
 * flink触发器
 * 1、基于时间触发和数据条数触发
 */
object Demo32_stream_trigger {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//附件时间 --- 处理时间类型
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
		//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		env.socketTextStream("192.168.5.101", 666)
			.flatMap(_.split(" "))
			.map((_,1))
			.keyBy(0)
			.timeWindow(Time.seconds(5))  //基于时间滚动窗口
			.trigger(new MyTrigger)
			.sum(1)
			.print("trigger---")
		//触发执行
		env.execute("window")
	}
}

/*
1、自定义需要实现Trigger
2、泛型是输入和窗口的类型
3、TriggerResult里面的值：
FIRE ： 对窗口进行触发计算操作，但是不清除窗口数据，即窗口数据任然会保留。
CONTINUE ： 对窗口不做任何操作。
FIRE_AND_PURGE ： 对窗口进行触发计算操作，同时将窗口的数据清除。
PURGE ： 将窗口数据进行清空。
 */
class MyTrigger extends Trigger[(String,Int),TimeWindow]{
	//计算条数 ---最好使用带状态的 --- "count", new Sum, LongSerializer.INSTANCE
	private val stateDesc: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long](
		"count",
		new MyReduce,
		classOf[Long]
	)

	//处理窗口中的每一个数据
	override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
		//基于时间触发的---定时器 --- window.maxTimestamp()窗口里面的最大时间戳
		ctx.registerProcessingTimeTimer(window.maxTimestamp())

		//基于条数触发
		val count: ReducingState[Long] = ctx.getPartitionedState(stateDesc)
		count.add(1L)
		if (count.get >= 5) {
			count.clear()
			println("通过数据条数触发窗口--->")
			return TriggerResult.FIRE  //触发执行
		}
		return TriggerResult.CONTINUE //什么不做
	}

	//基于处理时间定时器来调用方法
	override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
		println("通过处理时间触发窗口--->")
		TriggerResult.FIRE
	}

	//基于事件时间定时器来调用方法
	override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
		TriggerResult.CONTINUE
	}

	//清空窗口的时候被调用
	override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
		//删除定时器
		ctx.deleteProcessingTimeTimer(window.maxTimestamp())
	}
}


//自定义MyReduce操作
class MyReduce extends ReduceFunction[Long] {
	private val serialVersionUID: Long = 1L

	@throws[Exception]
	override def reduce (value1: Long, value2: Long): Long = {
		return value1 + value2
	}
}