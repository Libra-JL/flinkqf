package com.qianfeng.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row


object Demo04_table_batch {
	def main(args: Array[String]): Unit = {
		val batchEnvironment = ExecutionEnvironment.getExecutionEnvironment
		val batchTableEnvironment = BatchTableEnvironment.create(batchEnvironment)

		val ds: DataSet[(String,Int)] = batchEnvironment.readCsvFile(".....")

		import org.apache.flink.table.api.scala._
		val table: Table = batchTableEnvironment.fromDataSet(ds, 'name, 'age)


		table.groupBy('name)
    		.select('name,'age.sum as 'agesum)
    		.toDataSet[Row]
    		.print()

	}
}
