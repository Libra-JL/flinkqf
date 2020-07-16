package com.qianfeng.stream

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object Demo28_stream_Accumulator {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		env.socketTextStream("192.168.5.101", 666)
			.map(line => {
				//构造key-value数据
				val fields: Array[String] = line.split(" ")
				//key： date_province value：(add possible)
				val date: String = fields(0).trim
				val province: String = fields(1).trim
				val add: Int = fields(2).trim.toInt
				val possible: Int = fields(3).trim.toInt
				(date + "_" + province, add, possible)
			})
			.keyBy(0)
			.map(new RichMapFunction[(String, Int, Int), String] {

				//定义累加器
				val addCounter: IntCounter = new IntCounter(0)
				val possibleCounter: IntCounter = new IntCounter(0)

				//累加器注册
				override def open(parameters: Configuration): Unit = {
					getRuntimeContext.addAccumulator("add", addCounter)
					getRuntimeContext.addAccumulator("possible", possibleCounter)
				}

				//一行数据执行一次
				override def map(value: (String, Int, Int)): String = {
					//使用累加器
					addCounter.add(value._2)
					possibleCounter.add(value._3)
					//返回
					value + ""
				}
			}).print("accumulator---")

		//获取累加器值
		val result: JobExecutionResult = env.execute("acc")
		val adds: Int = result.getAccumulatorResult[Int]("add")
		val possibles: Int = result.getAccumulatorResult[Int]("possible")
		println(s"累计新增->$adds  累计其它->$possibles")
	}
}
