//package com.qianfeng.stream
//
//
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.time.Time
//
//
//object Demo31_stream_windowAggregate {
//	def main(args: Array[String]): Unit = {
//		//1、获取流式执行环境   --- scala包
//		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//
//		//附加时间
//		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//
//
//		env.socketTextStream("192.168.5.101", 666)
//			.map(line => {
//				val fields = line.split(" ")
//				val date = fields(0).trim
//				val provience = fields(1).trim
//				val addr = fields(2).trim
//				(date + "" + provience, addr)
//			})
//			.keyBy(0)
//			.timeWindow(Time.seconds(5))
//			.aggregate(new AggregateFunction[(String, Int), (String, Int, Int), (String, Double)] {
//				//累加器初始化
//				override def createAccumulator(): (String, Int, Int) = ("", 0, 0)
//
//				//单个累加器相加
//				override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
//					//输入条数
//					var cnt = accumulator._2 + 1
//					var adds = accumulator._3 + value._2
//					(value._1, cnt, adds)
//				}
//
//				override def getResult(accumulator: (String, Int, Int)): (String, Double) = {
//					(accumulator._1, accumulator._3 * 1.0 / accumulator._2)
//				}
//
//				override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
//					val add_cnt = a._2 + b._2
//					val adds = a._3 + b._3
//					(a._1, add_cnt, adds)
//				}
//			}).print("aggregate--")
//
//		//触发执行
//		env.execute("redis sink connector---")
//	}
//}
