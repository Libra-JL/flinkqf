package com.qianfeng.stream

import java.util.Properties

import com.qianfeng.common.{YQ, YQSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

/**
 *
 * 原始数据：
 * date province add possible
 * 2020-7-1 beijing 1 2
 * 2020-7-2 beijing 2 1
 * 2020-7-3 beijing 1 0
 * 2020-7-3 tianjin 2 1
 * *
 * 需求：
 * 1、算出每天、省份的adds、possible
 * 2、将如上计算结果打入到mysql中
 */
object Demo16_stream_kafkaSink {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment

		val res = environment.socketTextStream("192.168.5.101", 666)
			.map(line => {
				val fields = line.split(" ")
				val dt = fields(0).trim
				val provience = fields(1).trim
				val add = fields(2).trim.toInt
				val possible = fields(3).trim.toInt
				(dt + "_" + provience, (add, possible))

			})
			.keyBy(0)
			.reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
			.map(y => {
				val dp = y._1.split("_")
				new YQ(dp(0), dp(1), y._2._1, y._2._2)
			})


		val pro = new Properties()
		pro.put("bootstrap_servers","192.168.5.101:9092,192.168.5.102:9092,192.168.5.103:9092")
		val to_topic = "test"
		//topic
		val flinkKafkaProducer = new FlinkKafkaProducer[YQ](
			to_topic,
			new YQSchema(to_topic),
			pro,
			Semantic.EXACTLY_ONCE
		)


		res.addSink(flinkKafkaProducer)

		environment.execute("kafka  sink-")
	}


}

