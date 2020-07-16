package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object Demo01_table_watermark {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		val tableEnv = StreamTableEnvironment.create(env)

		val envDS = env.socketTextStream("192.168.5.101", 666)
			.filter(_.trim.nonEmpty)
			.map(line => {
				val strings = line.split(" ")
				val date = strings(0).trim
				val provience = strings(1).trim
				val add = strings(2).trim.toInt
				val possible = strings(3).trim.toInt
				(date, provience, add, possible)
			})

		val table = tableEnv.fromDataStream(envDS)

		val res = table.select("_1,_2,_3")
			.where("_3>10")

		tableEnv.toAppendStream[Row](res).print("table--")

		env.execute("table")
	}
}
