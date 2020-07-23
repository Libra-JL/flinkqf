//package com.qianfeng.stream
//
//import java.text.SimpleDateFormat
//
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import org.apache.flink.streaming.api.scala.function.RichWindowFunction
//import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//object Demo36_stream_outside {
//	def main(args: Array[String]): Unit = {
//		val env = StreamExecutionEnvironment.getExecutionEnvironment
//		env.setParallelism(1)
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//		val outputTag = new OutputTag[(String, Long)]("side_data")
//
//
//		val result = env.socketTextStream("192.168.5.101", 666)
//			.filter(_.nonEmpty)
//			.map(x => {
//				val strings = x.split(" ")
//				(strings(0).trim, strings(1).toLong)
//			}).assignTimestampsAndWatermarks(new MyWaterMarkAssinger1)
//			.keyBy(0)
//			.timeWindow(Time.seconds(3))
//			.allowedLateness(Time.seconds(2))
//			.sideOutputLateData(outputTag)
//			.apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
//
//				val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//
//				override def apply(key: Tuple,
//								   window: TimeWindow,
//								   input: Iterable[(String, Long)],
//								   out: Collector[String]): Unit = {
//
//					val lst = input.iterator.toList.sortBy(_._2)
//
//					val startTime = window.getStart
//					val endTime = window.getEnd
//					val res = s"key->${key.getField(0)}," +
//						s"事件开始时间EventTime->${fmt.format(lst.head._2)}," +
//						s"事件结束时间EventTime->${fmt.format(lst.last._2)}," +
//						s"窗口开始时间->${startTime}," +
//						s"窗口结束时间->${endTime}"
//
//					out.collect(res)
//				}
//			})
//
//
//		result.getSideOutput(outputTag).print("outsidedata")
//
//		//触发执行
//		env.execute("window")
//	}
//}
//
//class MyWaterMarkAssinger1 extends AssignerWithPeriodicWatermarks[(String, Int)] {
//	var maxTimeStamp = 0L
//	val lateness = 1000 * 10
//
//	val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//
//	override def getCurrentWatermark: Watermark = {
//		new Watermark(maxTimeStamp - lateness)
//	}
//
//	override def extractTimestamp(element: (String, Int), previousElementTimestamp: Long): Long = {
//		val now_time = element._2
//		maxTimeStamp = Math.max(now_time, maxTimeStamp)
//
//		val watermark_timestamp = getCurrentWatermark.getTimestamp
//		println(s"Event时间->${now_time} | ${fmt.format(now_time)}, " +
//			s"本窗口迄今为止最大的时间->${maxTimeStamp} | ${fmt.format(maxTimeStamp)}," +
//			s"当前watermark->${watermark_timestamp} | ${fmt.format(watermark_timestamp)}")
//
//
//		now_time
//	}
//}
//
//
//
//
