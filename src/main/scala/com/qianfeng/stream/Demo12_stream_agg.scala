//package com.qianfeng.stream
//
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.api.scala._
//import org.apache.flink.container.entrypoint.StandaloneJobClusterEntryPoint
//import org.apache.flink.runtime.taskexecutor.TaskManagerRunner
//import org.apache.flink.yarn.cli.FlinkYarnSessionCli
//object Demo12_stream_agg {
//	def main(args: Array[String]): Unit = {
//		val environment = StreamExecutionEnvironment.getExecutionEnvironment
//		val ds = environment.fromElements(Tuple2(200, 66), Tuple2(100, 66), Tuple2(200, 66), Tuple2(100, 66))
//		val keyds = ds.keyBy(0)
//
//		keyds.min(1)
//		keyds.minBy(1)
//		keyds.max(1)
//		keyds.maxBy(1)
//		keyds.sum(1)
//
//		environment.execute("agg--")
//
//
//	}
//}
