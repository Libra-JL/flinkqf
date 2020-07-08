package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

object Demo03_stream_customerSource {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment

		environment.fromElements("i like bigdata", "flink").print("fromelement-")

		val list = new ListBuffer[Int]()
		list += 10
		list += 20
		list += 33

		environment.fromCollection(list).filter(x=>x>=20).print("fromcollection-")
		println("collection==================================")
		environment.readTextFile("I:/HTTP_20130313143750.dat","UTF-8").print("readtextfile-")
		environment.readTextFile("hdfs://192.168.5.102:9000/test/wc.txt","UTF-8").print("readtextfile-hdfs-")
		println("file==================================")
		environment.socketTextStream("192.168.5.101",6666).setParallelism(1).print("socket-")
		println("socket==================================")
		environment.execute("basic stream")


	}
}
