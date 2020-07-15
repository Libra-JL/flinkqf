package com.qianfeng.stream

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner



object Demo22_stream_partitioner {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val ds = env.socketTextStream("192.168.5.101", 666)

		//shuffle
		ds.shuffle.print("shuffle---").setParallelism(4)
		//rebalance
		ds.rebalance.print("rebalance---").setParallelism(4)
		//.rescale
		ds.rebalance.rescale
		ds.rescale
		ds.global
		ds.broadcast
		ds.forward


		//触发执行
		env.execute("redis sink connector---")
	}
}
