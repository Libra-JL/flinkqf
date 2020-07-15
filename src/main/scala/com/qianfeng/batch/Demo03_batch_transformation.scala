package com.qianfeng.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.Path

import scala.collection.mutable.ListBuffer

object Demo03_batch_transformation {
	def main(args: Array[String]): Unit = {
		val env = ExecutionEnvironment.getExecutionEnvironment

		val text = env.fromElements("i like flink very much","lijie")

//		text.map((_,1)).print()
//
//		text.flatMap(_.split(" ")).print()
//
//		text.mapPartition(x=>x.map((_,1))).print()
//
//		text.filter(x=>x.length>10).print()

//		text.distinct().print()
//		text.flatMap(x=>x.split(" ")).map((_,1)).distinct(0).print("dis---")


		val data1: DataSet[(Int, String, Int)] = env.fromElements((1, "zs", 16), (1, "ls", 20), (2, "goudan", 23), (3, "mazi", 30))
		data1.aggregate(Aggregations.SUM,0).aggregate(Aggregations.MIN,2).print()


	}

}
