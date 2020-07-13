package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import com.qianfeng.common.YQ
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
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
object Demo14_stream_mysqloutput {
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


		res.writeUsingOutputFormat(new MyMysqlOutFormat)

		environment.execute("基础sink-")
	}


}


class MyMysqlOutFormat extends OutputFormat[YQ] {

	var conn: Connection = _
	var ps: PreparedStatement = _

	override def configure(parameters: Configuration): Unit = {

	}

	override def open(taskNumber: Int, numTasks: Int): Unit = {
		try {
			Class.forName("com.mysql.jdbc.Driver")
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/local", "root", "administrator")
		} catch {
			case e: SQLException => e.printStackTrace()
		}


	}

	override def writeRecord(record: YQ): Unit = {
		ps = conn.prepareStatement("replace into yq(dt,province,adds,possibles) values (?,?,?,?)")
		ps.setString(1,record.dt)
		ps.setString(2,record.provience)
		ps.setInt(3,record.adds)
		ps.setInt(4,record.possible)
		ps.execute()

	}

	override def close(): Unit = {
		if (ps != null) {
			ps.close()
		}
		if (conn != null) {
			conn.close()
		}
	}
}