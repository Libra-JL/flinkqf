package com.qianfeng.stream


import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row


/**
 * 使用flinkjdbc来读取有jdbc驱动数据库中数据(维度数据、读取增量数据场景)
 * 1、添加flink-jdbc的依赖
 *
 * 注:
 * 1、支持jdbc驱动的数据库都可以使用该方式读取
 * 2、flink-jdbc的底层其实是自定义InputFormat
 * 3、如果不支持jdbc的驱动，，可以进行自定义InputFormat
 * 4、整个InputFormat可以当成是flink的source，需要使用的是env.createInput()
 */
object Demo05_stream_flink_jdbc {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//2、自定义jdbc的source  --- 表中字段类型
		val fileds_type: Array[TypeInformation[_]] = Array[TypeInformation[_]](
			//id字段类型
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO
		)
		//获取行信息
		val rowTypeInfo: RowTypeInfo = new RowTypeInfo(fileds_type: _*)


		//获取flink-jdbc的InputFormat输入格式
		val jdbcInputFormat: JDBCInputFormat = JDBCInputFormat
			.buildJDBCInputFormat()
			.setDrivername("com.mysql.jdbc.Driver")
			.setDBUrl("jdbc:mysql://hadoop01:3306/test")
			.setUsername("root")
			.setPassword("root")
			.setQuery("select * from stu1")
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