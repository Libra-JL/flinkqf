package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.qianfeng.common.Stu
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Demo04_stream_customerSource_mysql {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment


//		val value = environment.addSource(new MysqlSource)
		val value = environment.addSource(new MysqlParallerSource).setParallelism(4)

		value.print("mysql source-")

		environment.execute("basic stream")


	}
}

class MysqlSource() extends RichSourceFunction[Stu] {

	var conn: Connection = _
	var ps: PreparedStatement = _
	var res: ResultSet = _

	override def open(parameters: Configuration): Unit = {
		val driver = "com.mysql.jdbc.Driver"
		val url = "jdbc:mysql://127.0.0.1:3306/local"
		val user = "root"
		val passwd = "administrator"

		try {
			Class.forName(driver)
			conn = DriverManager.getConnection(url, user, passwd)
			ps = conn.prepareStatement("select * from dmp")
		} catch {
			case e: Exception => e.printStackTrace()
		}

	}

	override def run(sourceContext: SourceFunction.SourceContext[Stu]): Unit = {
		try {
			res = ps.executeQuery()
			while (res.next()) {

				val stu = new Stu(res.getInt(1),
					res.getString(2).trim,
					res.getString(3).trim)

				sourceContext.collect(stu)
			}
		} catch {
			case e1: Exception => e1.printStackTrace()
		}
	}

	override def cancel(): Unit = {

	}

	override def close(): Unit = {
		if (res != null) {
			res.close()
		}
		if (ps != null) {
			ps.close()
		}
		if (conn != null) {
			conn.close()
		}
	}
}

class MysqlParallerSource extends RichParallelSourceFunction[Stu]{
	var conn: Connection = _
	var ps: PreparedStatement = _
	var res: ResultSet = _

	override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
		val driver = "com.mysql.jdbc.Driver"
		val url = "jdbc:mysql://127.0.0.1:3306/local"
		val user = "root"
		val passwd = "administrator"

		try {
			Class.forName(driver)
			conn = DriverManager.getConnection(url, user, passwd)
			ps = conn.prepareStatement("select * from dmp")
		} catch {
			case e: Exception => e.printStackTrace()
		}

		try {
			res = ps.executeQuery()
			while (res.next()) {

				val stu = new Stu(res.getInt(1),
					res.getString(2).trim,
					res.getString(3).trim)

				ctx.collect(stu)
			}
		} catch {
			case e1: Exception => e1.printStackTrace()
		}

	}

	override def cancel(): Unit = {
		if (res != null) {
			res.close()
		}
		if (ps != null) {
			ps.close()
		}
		if (conn != null) {
			conn.close()
		}
	}
}
