package com.qianfeng.stream

import com.qianfeng.common.TempInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._


/**
 * keyBy：DataStream → KeyedStream
 * 将所有数据记录中相同的key分配到同一个分区中去，内部使用散列分区实现；
 * 类似于sql中group by
 * 支持位置、支持字段、支持数组、支持POJO(对象)
 * key by通常和reduce|window搭配使用
 *
 *reduce ： KeyedStream → DataStream
 *聚合合并，将每个分区中数据进行聚合计算，返回结果值。
 */
object Demo11_stream_Reduce {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//定义kafka消费所需

		//val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
		val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 678))

		dstream.keyBy(0)
			.reduce((kv1,kv2)=>(kv1._1,kv1._2+kv2._2))
			.print("reduce---")
		//触发执行
		env.execute("reduce---")
	}
}
