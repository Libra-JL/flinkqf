package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 *
 * 原始数据：
 *date province add possible
 *2020-7-1 beijing 1 2
 *2020-7-2 beijing 2 1
 *2020-7-3 beijing 1 0
 *2020-7-3 tianjin 2 1
 **
 *需求：
 *1、算出每天、省份的adds、possible
 *2、将如上计算结果打入到redis中
 */
object Demo17_stream_ReidsSink {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//定义kafka消费所需
		val res: DataStream[(String, String)] = env.socketTextStream("192.168.5.101", 666)
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
				(date_province(0) + "_" + date_province(1), (y._2._1 + "_" + y._2._2))
			})

		//将res结果打入redis中
		val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost("192.168.5.103")
			.setPort(6379)
			.build()

		//获取redis的sink
		val myRedisSink: RedisSink[(String, String)] = new RedisSink(config, new MyRedisMapper)

		//将其添加到sink中
		res.addSink(myRedisSink)
		//触发执行
		env.execute("redis sink connector---")
	}
}

/**
 * 由于redis是一个k-v类型存储，，所以泛型最好是k-v类型
 *
 * key:时间和省份 维度组合
 * value : 新增等
 */
class MyRedisMapper() extends RedisMapper[(String,String)] {
	//redis命令描述器 --set还是hset，
	//1、additionalKey,,当我们存储为Hash或者sorted Set必须要设置额外的key
	override def getCommandDescription: RedisCommandDescription = {
		new RedisCommandDescription(RedisCommand.SET,null)
	}

	//存储到reds中的key
	override def getKeyFromData(data: (String, String)): String = data._1

	//存储到redis中的value
	override def getValueFromData(data: (String, String)): String = data._2
}
