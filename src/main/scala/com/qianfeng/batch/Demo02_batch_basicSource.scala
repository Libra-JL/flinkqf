package com.qianfeng.batch

import java.io.FileInputStream

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.collection.mutable.ListBuffer
object Demo02_batch_basicSource {
	def main(args: Array[String]): Unit = {
		val env = ExecutionEnvironment.getExecutionEnvironment

		env.fromElements("i like flink very much").print("fromelement---")

		val ints = new ListBuffer[Int]()
		ints.append(10,20,33)
		env.fromCollection(ints).print("fromcollection---")

		env.readTextFile("E:\\a.txt")
		env.readTextFile("hdfs://192.168.5.101:9000/a.txt")

		env.createInput(new TextInputFormat(new Path("hdfs://192.168.5.101:9000/a.txt")))

	}

}
