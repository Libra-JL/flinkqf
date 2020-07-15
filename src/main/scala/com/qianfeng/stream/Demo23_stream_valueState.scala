package com.qianfeng.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object Demo23_stream_valueState {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//		val ds = env.socketTextStream("192.168.5.101", 666)
		import org.apache.flink.api.scala._
		env.fromElements((1L, 5L), (1L, 6L), (1L, 10L), (2L, 3L), (2L, 8L))
			.keyBy(0)
			.flatMap(new MyFlatMapFunction())
			.print()

		//触发执行
		env.execute("redis sink connector---")
	}
}

class MyFlatMapFunction extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

	var sum: ValueState[(Long, Long)] = _

	override def open(parameters: Configuration): Unit = {
		val desc = new ValueStateDescriptor[(Long, Long)](
			"average",
			TypeInformation.of(new TypeHint[(Long, Long)] {}),
			(0l, 0l)
		)
		sum = getRuntimeContext.getState(desc)
	}


	override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {
		val currentSum = sum.value()
		val count = currentSum._1 + 1
		val sumed = currentSum._2 + value._2

		sum.update((count, sumed))
		//		out.collect((count, sumed))

		if (sum.value()._1 >= 2) {
			out.collect((value._1, sum.value()._2 / sum.value()._1))
			sum.clear()
		}
	}

}
