package com.qianfeng.stream

import com.qianfeng.common.TempInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


object Demo08_stream_SplitSelect {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val ds = env.socketTextStream("192.168.5.101", 666)

		val sstream = ds.map(personInfo => {
			val fields = personInfo.split(" ")
			TempInfo(fields(0).toInt, fields(1).trim, fields(2).toDouble, fields(3).toLong, fields(4).trim)
		}).split((temp: TempInfo) => if (temp.Temp >= 36.0 && temp.Temp <= 37.8) Seq("正常") else Seq("异常"))

		sstream.select("正常").print("正常旅客--")
		sstream.select("异常").print("异常旅客人--")

		env.execute("selectAndSplit--")


	}
}
