package com.qianfeng.stream

import com.qianfeng.common.YQDetail
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object Demo19_stream_Filesink {
	def main(args: Array[String]): Unit = {
		//		//1、获取流式执行环境   --- scala包
		//		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//		env.enableCheckpointing(10*1000)
		//		//定义kafka消费所需
		//		val res = env.socketTextStream("192.168.5.101", 666)
		//
		//		//落地位置
		//		val path = new Path("hdfs://192.168.5.101:9000/out/flink/streamingFile")
		//		//回滚策略
		//		val rollingPolicy:DefaultRollingPolicy[String,String] = DefaultRollingPolicy.create()
		//			.withRolloverInterval(10 * 1000)
		//			.withInactivityInterval(5 * 1000)
		//			.withMaxPartSize(128 * 1024 * 1024)
		//			.build()
		//
		//
		//
		//		//桶分配器
		//		val bucketAssinger = new DateTimeBucketAssigner[String]("yyyyMMddHH")
		//
		//		val sfs = StreamingFileSink
		//			.forRowFormat(path, new SimpleStringEncoder[String]("UTF-8"))
		//			.withBucketAssigner(bucketAssinger)
		//			.withRollingPolicy(rollingPolicy)
		//			.withBucketCheckInterval(10 * 1000)
		//			.build()
		//
		//
		//		res.addSink(sfs)
		//
		//		//触发执行
		//		env.execute("redis sink connector---")

		//------------------------------------------------------------------------------------------------
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.enableCheckpointing(10*1000)
		val res: DataStream[YQDetail] = env.socketTextStream("192.168.5.101", 666).map(x => {
			val fileds: Array[String] = x.split(" ")
			YQDetail(fileds(0), fileds(1), fileds(2).toInt, fileds(3).toInt)
		})

		//将其添加到sink中
		val outputPath: Path = new Path("hdfs://192.168.5.101:9000/out/flink/yq/dt=2020-07-11")

		//数据桶分配器
		val bucketAssinger: BasePathBucketAssigner[YQDetail] = new BasePathBucketAssigner()

		//获取FileSink
		val parquetFileSink: StreamingFileSink[YQDetail] = StreamingFileSink
			//块编码
			.forBulkFormat(
				outputPath,
				ParquetAvroWriters.forReflectRecord(classOf[YQDetail])) //指定使用parquet序列化
			.withBucketAssigner(bucketAssinger)
			.withBucketCheckInterval(10 * 1000) //桶检测间隔
			.build()
		//添加sink
		res.addSink(parquetFileSink)
		env.execute("lalal--")
	}
}
