package com.qianfeng.stream

import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Demo24_stream_Checkpoint {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)

		env.setStateBackend(new MemoryStateBackend(128 * 1024 * 1024, true))

		//		env.setStateBackend(new FsStateBackend("hdfs://192.168.5.101:9000/flink/1908_chickpoint"))

		//		env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.5.101:9000/flink/1908_chickpoint",true))

		val config = env.getCheckpointConfig
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
		config.setCheckpointTimeout(10 * 1000)
		config.setMaxConcurrentCheckpoints(1)


		import org.apache.flink.api.scala._
		env.socketTextStream("192.168.5.101", 666)
			.flatMap(_.split(" "))
			.map((_, 1))
			.keyBy(0)
			.sum(1)
			.print()

		env.execute("checkpoint--")
	}
}
