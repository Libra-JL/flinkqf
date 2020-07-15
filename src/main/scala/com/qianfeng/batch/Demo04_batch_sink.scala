package com.qianfeng.batch

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode


object Demo04_batch_sink {
	def main(args: Array[String]): Unit = {
		val environment = ExecutionEnvironment.getExecutionEnvironment
		val ds = environment.fromElements(Tuple2(200, 66), Tuple2(100, 66), Tuple2(200, 66), Tuple2(100, 66))

		val res = ds.groupBy(0).sum(1)


		//基于文件
		res.print("print sink---")
		res.writeAsText("E:\\flinkdata\\out\\0800",WriteMode.OVERWRITE)
		res.writeAsText("hdfs://hadoop01:9000/08t/06",WriteMode.OVERWRITE)
		res.writeAsCsv("hdfs://hadoop01:9000/08t/07",WriteMode.OVERWRITE)

		//output
		res.output(new MyBatchMysqlOutputFormat)

		environment.execute("基于文件")
	}
}
class MyBatchMysqlOutputFormat extends OutputFormat[(Int,Int)]{
	//连接数据库的对象
	var conn:Connection = _
	var ps:PreparedStatement = _
	override def configure(parameters: Configuration): Unit = {

	}

	override def open(taskNumber: Int, numTasks: Int): Unit = {
		val driver = "com.mysql.jdbc.Driver"
		val url = "jdbc:mysql://hadoop01:3306/test"
		val user = "root"
		val pass = "root"
		//反射
		try {
			Class.forName(driver)
			conn = DriverManager.getConnection(url, user, pass)
		} catch {
			case e:SQLException => e.printStackTrace()
		}
	}

	override def writeRecord(record: (Int, Int)): Unit = {
		ps = conn.prepareStatement("replace into wc(dt,province,adds,possibles) values(?,?)")
		ps.setInt(1,record._1)
		ps.setInt(2,record._2)
		ps.execute()
	}

	override def close(): Unit = {
		if(ps != null){
			ps.close()
		}
		if(conn != null){
			conn.close()
		}
	}

}