package com.qianfeng.stream

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo25_stream_Restart {
	def main(args: Array[String]): Unit = {
		//1、获取流式执行环境   --- scala包
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//checkpoint开启
		env.enableCheckpointing(10*1000,CheckpointingMode.AT_LEAST_ONCE)  //开启checkpoint

		//配置其它checkpoint相关参数
		val config: CheckpointConfig = env.getCheckpointConfig
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
		config.setCheckpointTimeout(30*1000) //超时设置
		config.setMaxConcurrentCheckpoints(1) //同时运行多少个检查点进行

		//状态后端存储
		env.setStateBackend(new MemoryStateBackend(128*1024*1024,true))

		//设置失败重启策略
		//1、失败不重启
		//env.setRestartStrategy(RestartStrategies.noRestart())
		//2、间隔性重启
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)))
		//3、失败率重启
		//env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(3),Time.seconds(10)))
		//4、fallback --- 默认就有，，会读取fllink-conf.yaml
		//env.setRestartStrategy(RestartStrategies.fallBackRestart())

		// 假设我们有一个 StreamExecutionEnvironment类型的变量 env
		import org.apache.flink.api.scala._
		env.socketTextStream("hadoop01",6666)
			.flatMap(_.split(" "))
			.map((_,1))
			.keyBy(0)
			.sum(1)
			.print()
		//5、触发执行  流应用一定要触发执行
		env.execute("fail restart---")
	}
}
