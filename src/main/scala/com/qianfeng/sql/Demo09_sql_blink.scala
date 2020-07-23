package com.qianfeng.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object Demo09_sql_blink {
	def main(args: Array[String]): Unit = {
		val settings = EnvironmentSettings
			.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build()
		val environment = TableEnvironment.create(settings)

		environment.sqlUpdate(
			s"""
			   |CREATE TABLE kafkaTable (
			   | user_id VARCHAR,
			   | item_id VARCHAR,
			   | category_id VARCHAR,
			   | behavior STRING,
			   | ts TIMESTAMP
			   |) WITH (
			   | 'connector.type' = 'kafka', -- 使用kafka的连接器
			   | 'connector.version' = 'universal', -- universal代表使用0.11以后，如果0.9则直接写0.9
			   | 'connector.topic' = 'test',
			   | 'connector.properties.0.key' = 'zookeeper.connect',
			   | 'connector.properties.0.value' = '192.168.5.101:2181,192.168.5.102:2181,192.168.5.103:2181',
			   | 'connector.properties.1.key' = 'bootstrap.servers',
			   | 'connector.properties.1.value' = '192.168.5.101:9092,192.168.5.102:9092,192.168.5.103:9092',
			   | 'update-mode' = 'append', -- 数据更新以追加形式
			   | 'connector.startup-mode' = 'latest-offset', //数据为主题中最新数据
			   | 'format.type' = 'json', -- 数据格式为json
			   | 'format.derive-schema' = 'true'  -- 从DDL schemal确定json解析规则
			   |)
			   |""".stripMargin)


		environment.sqlUpdate(
			s"""
			   |insert into user_report
			   |select
			   |DATE_FORMAT(ts,'yyyy-MM-dd HH:00') as dt,
			   |count(*) as pv,
			   |count(distinct(user_id)) as uv
			   |from kafkaTable
			   |group by DATE_FORMAT(ts,'yyyy-MM-dd HH:00')
			   |""".stripMargin
		)


		environment.sqlUpdate(
			s"""
			   |create table user_report(
			   |dt VARCHAR,
			   |pv BIGINT,
			   |uv BIGINT
			   |)WITH (
			   | 'connector.type' = 'jdbc',
			   | 'connector.url' = 'jdbc:mysql://localhost:3306/local',
			   | 'connector.table' = 'user_report',
			   | 'connector.username' = 'root',
			   | 'connector.password' = 'administrator',
			   | 'connector.write.flush.max-rows' = '1'
			   |)
			   |""".stripMargin
		)


		environment.execute("blink  kafka2mysql")

	}
}
