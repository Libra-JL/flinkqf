package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Demo01_stream_WordCount {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val dataStream:DataStream[String] = env.socketTextStream("192.168.5.101", 6666)

		val value = dataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)


		value.print("wc-")

		env.execute("my_wordcount")

	}
}
