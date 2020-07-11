package com.qianfeng.stream

import com.qianfeng.common.TempInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo10_stream_connect {
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

		//使用union进行合并
		val connectedStream = ds1.connect(ds3)
		connectedStream.map(
			common1 => ("正常旅客id:" + common1.uid + " 姓名:" + common1.uname),
			execption1 => ("异常旅客id:" + execption1.uid + " 姓名:" + execption1.uname + " 温度:" + execption1.Temp)
		).print("connected1 ---")


		env.execute("selectAndSplit--")
	}
}
