package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner

object Demo21_stream_chain {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		val res = env.fromElements("i like flink")
		res.map(_.split(" ")).startNewChain().flatMap(_.toList).print()
		res.map((_,1)).disableChaining().print()
		res.map((_,1)).slotSharingGroup("default")


		env.execute("chaining")
	}
}
