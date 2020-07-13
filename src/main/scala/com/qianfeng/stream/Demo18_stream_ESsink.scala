package com.qianfeng.stream

import java.util

import com.qianfeng.common.YQ
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


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
 * 2、将如上计算结果打入到redis中
 */
object Demo18_stream_ESsink {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//定义kafka消费所需
		val res = env.socketTextStream("192.168.5.101", 666)
			.map(line => {
				//构造key-value数据
				val fields: Array[String] = line.split(" ")
				//key： date_province value：(add possible)
				val date: String = fields(0).trim
				val province: String = fields(1).trim
				val add: Int = fields(2).trim.toInt
				val possible: Int = fields(3).trim.toInt
				(date + "_" + province, (add, possible))
			})
			.keyBy(0)
			.reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
			.map(y => {
				val date_province: Array[String] = y._1.split("_")
				YQ(date_province(0), date_province(1), y._2._1, y._2._2)
			})

		val hosts = new util.ArrayList[HttpHost]()
		hosts.add(new HttpHost("192.168.5.101", 9200, "http"))
		hosts.add(new HttpHost("192.168.5.102", 9200, "http"))
		hosts.add(new HttpHost("192.168.5.103", 9200, "http"))

		val esBuilder = new ElasticsearchSink.Builder[YQ](hosts, new MyESSink)
		esBuilder.setBulkFlushMaxActions(1)
		val esSink = esBuilder.build()

		res.addSink(esSink)


		//触发执行
		env.execute("redis sink connector---")
	}
}

class MyESSink() extends ElasticsearchSinkFunction[YQ] {
	override def process(t: YQ, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
		val map = new util.HashMap[String, String]()
		map.put("dt", t.dt)
		map.put("provience", t.provience)
		map.put("adds", t.adds + "")
		map.put("possible", t.possible + "")

		val indexRequest = Requests.indexRequest()
			.index("yq_report_daily")
			.`type`("info")
			.id(t.dt + "_" + t.provience)
			.source(map)

		requestIndexer.add(indexRequest)

	}
}
