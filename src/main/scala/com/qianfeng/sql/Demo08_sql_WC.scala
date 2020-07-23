package com.qianfeng.sql

import com.qianfeng.common.{WC, WordCount}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row



object Demo08_sql_WC {
	def main(args: Array[String]): Unit = {
		//1、获取流式表执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

		val ds = env.socketTextStream("192.168.5.101", 666)
			.filter(_.nonEmpty)
			.flatMap(_.split(" "))
			.map(x=>
				WC(x, 1)
			)
		import org.apache.flink.table.api.scala._
		val table = tenv.fromDataStream(ds, 'word, 'cnt)


		tenv.sqlQuery(
			s"""
			   |select
			   |word,
			   |sum(cnt)
			   |from $table
			   |group by word
			   |""".stripMargin
		).toRetractStream[Row]
    		.print("wc-->")

		env.execute("wc")
	}
}
