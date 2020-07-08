package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}

import scala.util.Random


object Demo02_stream_basicSource {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment

		val value = environment.addSource(new SourceFunction[String] {

			//生产数据传往下游

			override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
				val random = new Random()
				while (true) {
					val i = random.nextInt(100)
					sourceContext.collect("随机年龄" + i)
					Thread.sleep(i)
				}
			}

			//取消生产数据
			override def cancel(): Unit = {

			}
		})

		value.print("my source function")

//		SourceFunction
//
//		RichSourceFunction
//
//		ParallelSourceFunction
//
//		RichParallelSourceFunction

		environment.execute("basic stream")


	}
}
