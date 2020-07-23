package com.qianfeng.sql

import com.qianfeng.common.YQTimeSatmp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Slide, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object Demo07_sql_SlidWindow {
	def main(args: Array[String]): Unit = {
		//1、获取流式表执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

		//使用水印--
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		val ds: DataStream[YQTimeSatmp] = env.socketTextStream("192.168.5.101", 666)
			.filter(_.trim.nonEmpty)
			.map(line => {
				//构造key-value数据
				val fields: Array[String] = line.split(" ")
				//key： date_province value：(add possible)
				val date: String = fields(0).trim
				val province: String = fields(1).trim
				val add: Int = fields(2).trim.toInt
				val possible: Int = fields(3).trim.toInt
				val timestamp: Long = fields(4).trim.toLong
				YQTimeSatmp(date, timestamp, province, add, possible)
			})
			.assignTimestampsAndWatermarks(new timestamps.BoundedOutOfOrdernessTimestampExtractor[YQTimeSatmp](Time.seconds(2)) {
				override def extractTimestamp(element: YQTimeSatmp): Long = {
					element.timestamp
				}
			})

		import org.apache.flink.table.api.scala._
		val table = tenv.fromDataStream(ds, 'dt, 'provience, 'adds, 'ts.rowtime)

		//table api 滑动窗口
		//		val table1 = table.window(Slide over 10.second every 5.second on 'ts as 'tt)
		//			.groupBy('dt, 'dt.count)
		//			.select('dt, 'dt.count)
		//
		//
		//		tenv.toAppendStream[Row](table1)

		//sql 滑动窗口

		tenv.sqlQuery(
			s"""
			   |select
			   |provience,
			   |sum(adds)
			   |from $table
			   |group by provience,HOP(ts,interval '10' second,interval '5' second)
			   |""".stripMargin
		).toAppendStream[Row]
			.print("基于SQL")
		env.execute("wordcount")


	}
}
