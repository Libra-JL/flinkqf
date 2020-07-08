package com.qianfeng.stream


import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row

object Demo05_stream_flink_jdbc {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//2、自定义jdbc的source  --- 表中字段类型
		val fileds_type: Array[TypeInformation[_]] = Array[TypeInformation[_]](
			//id字段类型
			BasicTypeInfo.LONG_TYPE_INFO,
			//省
			BasicTypeInfo.STRING_TYPE_INFO,
			//市
			BasicTypeInfo.STRING_TYPE_INFO
		)
		//获取行信息
		val rowTypeInfo: RowTypeInfo = new RowTypeInfo(fileds_type: _*)

		//获取flink-jdbc的InputFormat输入格式
		val jdbcInputFormat: JDBCInputFormat = JDBCInputFormat
			.buildJDBCInputFormat()
			.setDrivername("com.mysql.jdbc.Driver")
			.setDBUrl("jdbc:mysql://localhost:3306/local")
			.setUsername("root")
			.setPassword("administrator")
			.setQuery("select * from dmp")
			.setRowTypeInfo(rowTypeInfo)
			.finish()

		//获取jdbc-inputformat中的数据
		val res: DataStream[Row] = env.createInput(jdbcInputFormat)
		//持久化
		res.print("flink-jdbc-")

		//触发执行
		env.execute("flink jdbc inputformat-")
	}
}

//class MysqlSource() extends RichSourceFunction[Stu] {
//
//	var conn: Connection = _
//	var ps: PreparedStatement = _
//	var res: ResultSet = _
//
//	override def open(parameters: Configuration): Unit = {
//		val driver = "com.mysql.jdbc.Driver"
//		val url = "jdbc:mysql://127.0.0.1:3306/local"
//		val user = "root"
//		val passwd = "administrator"
//
//		try {
//			Class.forName(driver)
//			conn = DriverManager.getConnection(url, user, passwd)
//			ps = conn.prepareStatement("select * from dmp")
//		} catch {
//			case e: Exception => e.printStackTrace()
//		}
//
//	}
//
//	override def run(sourceContext: SourceFunction.SourceContext[Stu]): Unit = {
//		try {
//			res = ps.executeQuery()
//			while (res.next()) {
//
//				val stu = new Stu(res.getInt(1),
//					res.getString(2).trim,
//					res.getString(3).trim)
//
//				sourceContext.collect(stu)
//			}
//		} catch {
//			case e1: Exception => e1.printStackTrace()
//		}
//	}
//
//	override def cancel(): Unit = {
//
//	}
//
//	override def close(): Unit = {
//		if (res != null) {
//			res.close()
//		}
//		if (ps != null) {
//			ps.close()
//		}
//		if (conn != null) {
//			conn.close()
//		}
//	}
//}
