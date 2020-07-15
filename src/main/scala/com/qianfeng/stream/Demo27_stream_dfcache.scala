package com.qianfeng.stream

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object Demo27_stream_dfcache {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		env.registerCachedFile("hdfs://192.168.5.101:9000/flink_cache/sex", "genderInfo")

		env.socketTextStream("192.168.5.101", 666)
			.map(new RichMapFunction[String, (String, String, Char, String)] {

				val map: mutable.HashMap[Int, Char] = mutable.HashMap()

				var bs: BufferedSource = _

				override def map(value: String): (String, String, Char, String) = {
					val fields = value.split(" ")
					val uid = fields(0).trim
					val uname = fields(1).trim
					val genderFlag = fields(2).toInt
					val addr = fields(3).trim

					val genderLab:Char = map.getOrElse(genderFlag, 'o')
					(uid,uname,genderLab,addr)
				}


				override def open(parameters: Configuration): Unit = {
					//获取缓存文件
					val cacheFile = getRuntimeContext.getDistributedCache.getFile("genderInfo")
					//读文件放入流
					bs = Source.fromFile(cacheFile)
					//讲流数据放入map
					val lines = bs.getLines().toList
					for (elem <- lines) {
						val fields = elem.split(" ")
						val genderFlag = fields(0).trim.toInt
						val genderLab = fields(1).trim.toCharArray()(0)
						map.put(genderFlag,genderLab)
					}
				}

				override def close(): Unit = super.close()
			})



		//触发执行
		env.execute("redis sink connector---")
	}
}
