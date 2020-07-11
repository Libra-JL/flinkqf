package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
object Demo13_stream_sink {
	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment
		val ds = environment.fromElements(Tuple2(200, 66), Tuple2(100, 66), Tuple2(200, 66), Tuple2(100, 66))

		val res = ds.keyBy(0)
			.reduce((kv1, kv2) => (kv1._1, kv1._2 + kv2._2))


		res.print("print sink----")
		res.writeAsCsv("out/csv.csv",WriteMode.OVERWRITE)
		res.writeAsText("out/text.txt",WriteMode.OVERWRITE)

		environment.execute("基于文件")
	}
}
