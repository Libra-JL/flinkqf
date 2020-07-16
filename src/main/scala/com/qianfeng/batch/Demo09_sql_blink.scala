package com.qianfeng.batch

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object Demo09_sql_blink {
	def main(args: Array[String]): Unit = {

		val settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build()
		val tenv = TableEnvironment.create(settings)

		tenv.sqlUpdate(
			s"""
			   |
			   |""".stripMargin
		)

		tenv.sqlUpdate(
			s"""
			   |
			   |""".stripMargin
		)

		tenv.sqlUpdate(
			s"""
			   |
			   |""".stripMargin
		)

	}
}
