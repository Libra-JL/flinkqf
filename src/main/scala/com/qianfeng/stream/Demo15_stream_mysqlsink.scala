package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import com.qianfeng.common.YQ
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * 原始数据：
 * date province add possible
 * 2020-7-1 beijing 1 2
 * 2020-7-2 beijing 2 1
 * 2020-7-3 beijing 1 0
 * 2020-7-3 tianjin 2 1
 * *
 * 需求：
 * 1、算出每天、省份的adds、possible
 * 2、将如上计算结果打入到mysql中
 */
object Demo15_stream_mysqlSink {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment
		val res = environment.socketTextStream("192.168.5.101", 666)
			.map(line => {
				val fields = line.split(" ")
				val dt = fields(0).trim
				val provience = fields(1).trim
				val add = fields(2).trim.toInt
				val possible = fields(3).trim.toInt
				(dt + "_" + provience, (add, possible))

			})
			.keyBy(0)
			.reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
			.map(y => {
				val dp = y._1.split("_")
				new YQ(dp(0), dp(1), y._2._1, y._2._2)
			})


		res.addSink(new MyMysqlSink)

		environment.execute("基础sink-")
	}


}


class MyMysqlSink extends RichSinkFunction[YQ] {
	var conn: Connection = _
	var ps: PreparedStatement = _

	override def open(parameters: Configuration): Unit = {
		try {
			Class.forName("com.mysql.jdbc.Driver")
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/local", "root", "administrator")
		} catch {
			case e: SQLException => e.printStackTrace()
		}

	}

	override def close(): Unit = {
		if (ps != null) {
			ps.close()
		}
		if (conn != null) {
			conn.close()
		}
	}

	override def invoke(value: YQ, context: SinkFunction.Context[_]): Unit = {
		ps = conn.prepareStatement("replace into yq(dt,province,adds,possibles) values (?,?,?,?)")
		ps.setString(1,value.dt)
		ps.setString(2,value.provience)
		ps.setInt(3,value.adds)
		ps.setInt(4,value.possible)
		ps.execute()
	}
}