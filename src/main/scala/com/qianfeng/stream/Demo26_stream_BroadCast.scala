package com.qianfeng.stream

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object Demo26_stream_BroadCast {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val dataStream1 = env.fromElements((1, '男'), (2, '女'), (3, '妖'))
		val dataStream2 = env.socketTextStream("192.168.5.101", 666)
			.map(line => {
				val strings = line.split(" ")
				(strings(0), strings(1), strings(2).toInt, strings(3))
			})

		val genderinfo = new MapStateDescriptor[Integer, Character](
			"genderinfo",
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.CHAR_TYPE_INFO
		)

		val brodStream = dataStream1.broadcast(genderinfo)

		dataStream2.connect(brodStream)
			.process(new BroadcastProcessFunction[(String, String, Int, String), (Int, Char), (String, String, Char, String)] {
				override def processElement(
											   value: (String, String, Int, String),
											   ctx: BroadcastProcessFunction[(String, String, Int, String), (Int, Char), (String, String, Char, String)]#ReadOnlyContext,
											   out: Collector[(String, String, Char, String)]): Unit = {
					val flag = value._3
					var gender = ctx.getBroadcastState(genderinfo).get(flag)
					if (gender == null) {
						gender = 'o'

					}
					out.collect((value._1, value._2, gender, value._4))
				}

				override def processBroadcastElement(
														value: (Int, Char),
														ctx: BroadcastProcessFunction[(String, String, Int, String), (Int, Char), (String, String, Char, String)]#Context,
														out: Collector[(String, String, Char, String)]): Unit = {
					ctx.getBroadcastState(genderinfo).put(value._1, value._2)
				}
			}).print("broadcast ---")

		//5、触发执行  流应用一定要触发执行
		env.execute("broadcast---")


	}

}
