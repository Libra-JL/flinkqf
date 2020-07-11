package com.qianfeng.stream

import com.qianfeng.common.WordCount
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


object Demo07_stream_filter {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val ds = env.socketTextStream("192.168.5.101", 666)

		val res = ds.flatMap(_.split(" "))
    		.filter(_.length>5)
			.map(word=>WordCount(word,1))
			.keyBy("word")
			.sum("count")

		res.print("opreate filter-")

		//触发执行
		env.execute("flink kafka inputformat-")
	}
}
