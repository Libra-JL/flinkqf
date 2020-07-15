package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 设置并行度
 */
object Demo20_stream_Paralle {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//env.setParallelism(30) --- 整个应用(每个算子)的并行度
		//2、获取数据源 --- source
		val dstream: DataStream[String] = env.socketTextStream("192.168.5.101", 666)
		val value: DataStream[(String, Int)] = dstream.flatMap(_.split(" "))
			.map((_, 1)) //(hello,1)
			.setParallelism(30)
			.keyBy(0) //分组，类似group by
		//.setParallelism(15)

		val sumed: DataStream[(String, Int)] = value.keyBy(0)
			//.timeWindow(Time.seconds(5),Time.seconds(2)) //滑动窗口
			.sum(1)

		val res: DataStream[(String, Int)] = sumed.setParallelism(15)
		sumed.setMaxParallelism(15)  //设置最大并行度
		//4、持久化 --- sink
		res.print("wc-").setParallelism(5)//.setParallelism(1)

		//5、触发执行  流应用一定要触发执行
		env.execute("setParallelism")
	}
}


