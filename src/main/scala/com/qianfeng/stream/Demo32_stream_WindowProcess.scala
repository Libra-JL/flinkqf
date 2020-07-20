package com.qianfeng.stream


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Demo32_stream_WindowProcess {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


		env.socketTextStream("192.168.5.101", 666)
			.map(line => {
				val strings = line.split(" ")
				val date = strings(0)
				val provience = strings(1)
				val add = strings(1).toInt
				(date + "_" + provience, add)

			})
			.keyBy(0)
    		.timeWindow(Time.seconds(10),Time.seconds(5))
			.process[(String,Int)](new ProcessWindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {
				override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
					var cnt: Int = 0
					var adds: Int = 0

					elements.foreach(line => {
						cnt = cnt + 1
						adds = adds + line._2
					})
					out.collect((key.getField(0), adds / cnt))

				}
			})


		//触发执行
		env.execute("redis sink connector---")
	}
}
