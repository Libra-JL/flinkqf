package com.qianfeng.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object Demo01_batchWC {
	def main(args: Array[String]): Unit = {

		if (args == null && args.length != 2) {
			System.exit(-1)
		}

		val environment = ExecutionEnvironment.getExecutionEnvironment

		val ds = environment.fromElements(args(0))

		val value = ds.flatMap(_.split(" "))
			.map((_, 1))
			.groupBy(0)
			.sum(1)

		value.print()
		value.writeAsText(args(1))


	}
}
