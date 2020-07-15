package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.concurrent.TimeUnit

import com.qianfeng.common.Stu
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.api.scala._



object Demo25_stream_AsyncIO {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val resDS:DataStream[Stu] = env.addSource(new MysqlSource)

//		val result:DataStream[Stu] = AsyncDataStream.unorderedWait(
//			resDS,
//			new AsyncDatabaseRequest(),
//			1000L,
//			TimeUnit.MILLISECONDS,
//			100
//		)

		env.execute("fail restart---")
	}
}


class AsyncDatabaseRequest extends RichAsyncFunction[String,Stu]{
	var conn: Connection = _
	var ps: PreparedStatement = _
	var res: ResultSet = _

	override def asyncInvoke(input: String, resultFuture: ResultFuture[Stu]): Unit = {
		try {
			res = ps.executeQuery()
			val stus = new util.ArrayList[Stu]()

			while (res.next()) {
				val stu = new Stu(res.getInt(1),
					res.getString(2).trim,
					res.getString(3).trim
				)
				stus.add(stu)
			}
			resultFuture.complete(stus.asInstanceOf[Iterable[Stu]])
		} catch {
			case e1: Exception => e1.printStackTrace()
		}
	}

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


