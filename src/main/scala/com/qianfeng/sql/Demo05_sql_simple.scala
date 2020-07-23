package com.qianfeng.sql

import com.qianfeng.common.YQTimeSatmp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 表的sql操作
 */
object Demo05_sql_simple {
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
				val timestamp = strings(4).toLong
				YQTimeSatmp(date, timestamp, provience, add, possible)
			})

		val table = tableEnv.fromDataStream[YQTimeSatmp](envDS)

		import org.apache.flink.table.api.scala._
		tableEnv.sqlQuery(
			s"""
			   |select
			   |*
			   |from $table
			   |where adds>10
			   |""".stripMargin
		).toAppendStream[Row]
			.print("sql ----")


		env.execute("table window watermark")
	}
}
