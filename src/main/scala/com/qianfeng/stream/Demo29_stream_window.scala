package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time


object Demo29_stream_window {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


		//附加时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


		env.socketTextStream("192.168.5.101", 666)
			.flatMap(_.split(" "))
			.map((_, 1))
			.keyBy(0)
			//    			.timeWindow(Time.seconds(5))
			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
			//    			.timeWindow(Time.seconds(5),Time.seconds(2))
			//    			.countWindow(10)
			//    			.countWindow(5,3)
			//    			.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
			.sum(1)
			.print("window---")

		//触发执行
		env.execute("redis sink connector---")
	}
}



