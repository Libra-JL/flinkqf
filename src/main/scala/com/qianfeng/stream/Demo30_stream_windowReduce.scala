package com.qianfeng.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time



object Demo30_stream_windowReduce {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


		//附加时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


		env.socketTextStream("192.168.5.101",666)
    			.map(line=>{
					val fields = line.split(" ")
					val date = fields(0).trim
					val provience = fields(1).trim
					val addr = fields(2).trim
					(date+""+provience,addr)
				})
    			.keyBy(0)
    			.timeWindow(Time.seconds(5))
    			.reduce(new ReduceFunction[(String, String)] {
					override def reduce(value1: (String, String), value2: (String, String)): (String, String) = {
						if (value1._2>value2._2){
							value1
						}else{
							value2
						}
					}
				})

		//触发执行
		env.execute("redis sink connector---")
	}
}
