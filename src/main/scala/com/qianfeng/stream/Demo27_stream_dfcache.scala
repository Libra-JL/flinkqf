package com.qianfeng.stream

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object Demo27_stream_dfcache {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		//  1  男     2   女     3   妖
		env.registerCachedFile("hdfs://192.168.5.101:9000/cache/gender", "genderCode")

		env.socketTextStream("192.168.5.101", 666)
			.map(new RichMapFunction[String, (String, String, Char, String)] {

				val map: mutable.HashMap[Int, Char] = mutable.HashMap()
				var bs: BufferedSource = _

				override def map(value: String): (String, String, Char, String) = {
					val strings = value.split(" ")
					val uid = strings(0).trim
					val uname = strings(0).trim
					val gender = strings(0).trim.toInt
					val addr = strings(0).trim
					val sex = map.getOrElse(gender, 'X')
					(uid, uname, sex, addr)
				}

				override def open(parameters: Configuration): Unit = {
					val genderCodeFile = getRuntimeContext.getDistributedCache.getFile("genderCode")
					bs = Source.fromFile(genderCodeFile)
					val lines = bs.getLines()
					for (line <- lines) {
						val strings = line.split(" ")
						val flag = strings(0).trim.toInt
						val lab = strings(1).trim.toCharArray()(0)
						map.put(flag, lab)
					}

				}

				override def close(): Unit = {
					if (bs != null) {
						bs.close()
					}
				}
			}).print("cachefile---")

		env.execute("cachefile")
	}
}
