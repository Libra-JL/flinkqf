package com.qianfeng.stream

import com.qianfeng.common.TempInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Demo09_stream_Union {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val ds = env.socketTextStream("192.168.5.101", 666)

		val sstream = ds.map(personInfo => {
			val fields = personInfo.split(" ")
			TempInfo(fields(0).toInt, fields(1).trim, fields(2).toDouble, fields(3).toLong, fields(4).trim)
		}).split((temp: TempInfo) => if (temp.Temp >= 36.0 && temp.Temp <= 37.8) Seq("正常") else Seq("异常"))

		val ds1 = sstream.select("正常")

		val ds2 = sstream.select("正常")
		val ds3 = sstream.select("异常")

		ds1.union(ds2,ds3).print("union--")


		env.execute("selectAndSplit--")


	}
}
