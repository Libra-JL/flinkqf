package com.qianfeng.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable


object Demo05_batch_broadcast {
	def main(args: Array[String]): Unit = {
		val env = ExecutionEnvironment.getExecutionEnvironment

		val ds1: DataSet[Map[Int, Char]] = env.fromElements((1, '男'), (2, '女')).map(Map(_))
		val ds2 = env.fromElements((101, "张无忌", 1, "湖北省武汉市"), (112, "marry", 2, "北京市昌平区"), (115, "tom", 1, "北京市朝阳区"))

		ds2.map(new RichMapFunction[(Int, String, Int, String), (Int, String, Char, String)] {
			var bc: java.util.List[Map[Int, Char]] = _
			var bcTmp: mutable.Map[Int, Char] = _

			//初始化广播变量
			override def open(parameters: Configuration): Unit = {
				import scala.collection.JavaConversions._
				bcTmp = mutable.HashMap()
				//获取广播变量
				bc = getRuntimeContext.getBroadcastVariable("genderInfo")
				//将广播变量循环放到临时map中
				for (perEle <- bc) bcTmp.putAll(perEle)
			}

			//一行数据映射一次
			override def map(value: (Int, String, Int, String)): (Int, String, Char, String) = {
				val gender = bcTmp.getOrElse(value._3, '妖')
				(value._1, value._2, gender, value._4)
			}
		})
			.withBroadcastSet(ds1, "genderInfo")   //使用广播变量
			.print()
	}
}