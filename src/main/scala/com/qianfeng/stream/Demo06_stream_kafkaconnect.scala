package com.qianfeng.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Demo06_stream_kafkaconnect {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val from_topic = "test"
		val properties = new Properties()
		properties.load(Demo06_stream_kafkaconnect.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
		println(properties.getProperty("bootstrap.server"))
		val value = new FlinkKafkaConsumer(from_topic, new SimpleStringSchema(), properties)
		value.setStartFromLatest()
		env.addSource(value).print("kafka-")

		//触发执行
		env.execute("flink kafka inputformat-")
	}
}
