package com.qianfeng.common

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class YQSchema(topic: String) extends KafkaSerializationSchema[YQ] {
	override def serialize(t: YQ, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
		val dt = t.dt
		val provience = t.provience
		val adds = t.adds.toInt
		val possible = t.possible.toInt

		new ProducerRecord[Array[Byte], Array[Byte]](topic, (dt + "_" + provience + "_" + adds + "_" + possible).getBytes)
	}
}
