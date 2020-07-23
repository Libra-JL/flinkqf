package com.qianfeng.sql

import com.qianfeng.common.YQTimeSatmp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 表的水印操作
 */
object Demo01_table_watermark {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)

		val tableEnv = StreamTableEnvironment.create(env)

		val envDS = env.socketTextStream("192.168.5.101", 666)
			.filter(_.trim.nonEmpty)
			.map(line => {
				val strings = line.split(" ")
				val date = strings(0).trim
				val timestamp = strings(1).toLong
				val provience = strings(2).trim
				val add = strings(3).trim.toInt
				val possible = strings(4).trim.toInt
				YQTimeSatmp(date, timestamp, provience, add, possible)
			})
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[YQTimeSatmp](Time.seconds(2)) {
				override def extractTimestamp(element: YQTimeSatmp): Long = {
					element.timestamp
				}
			})
		import org.apache.flink.table.api.scala._
		val table = tableEnv.fromDataStream(envDS, 'dt, 'timestamp.rowtime,'adds)

		val table1 = table.window(Tumble over 2.second on 'timestamp as 'tt)
			.groupBy('dt, 'tt)
			.select('dt, 'dt.count, 'adds.sum)


		tableEnv.toAppendStream[Row](table1).print("table window watermark--")

		env.execute("table window watermark")
	}
}


/**
 * 打点水印
 * 打点水印比周期性水印用的要少不少，并且Flink没有内置的实现，那么就写个最简单的栗子吧。
 *
 *     sourceStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<UserActionRecord>() {
 *
 * @Nullable
 * @Override
 * public Watermark checkAndGetNextWatermark(UserActionRecord lastElement, long extractedTimestamp) {
 * return lastElement.getUserId().endsWith("0") ? new Watermark(extractedTimestamp - 1) : null;
 * }
 * @Override
 * public long extractTimestamp(UserActionRecord element, long previousElementTimestamp) {
 * return element.getTimestamp();
 * }
 * });
 *

 */