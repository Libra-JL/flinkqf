package com.qianfeng.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row

object Demo07_stream_filter {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val from_topic = "test"
		val properties = new Properties()
		properties.load(Demo06_stream_kafkaconnect.getClass.getClassLoader.getResourceAsStream("consumer.properties"))



		env.addSource(new FlinkKafkaConsumer(from_topic,new SimpleStringSchema(),properties)).print("kafka-")

		//触发执行
		env.execute("flink kafka inputformat-")
	}
}
