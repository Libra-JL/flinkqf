package com.qianfeng.stream

import java.util.Calendar

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object Other {

	def main(args: Array[String]): Unit = {
		val environment = StreamExecutionEnvironment.getExecutionEnvironment
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		environment.getConfig.setAutoWatermarkInterval(5000L)

		val ds = environment.addSource(new StockPriceSource)
			.assignTimestampsAndWatermarks(new StockPriceTimeAssigner)


		ds.keyBy(_.symbol)
    		.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    		.max("price").print("lalala")

		environment.execute("dsadas")
	}
}

case class StockPrice(symbol: String, timeStamp: Long, price: Double)

class StockPriceSource extends RichSourceFunction[StockPrice] {

	var isRunning = true

	val rand = new Random()

	var priceList: List[Double] = List(100.0d, 200.0d, 300.0d, 400.0d, 500.0d)
	var stockId = 0
	var curPrice = 0.0d

	override def run(ctx: SourceFunction.SourceContext[StockPrice]): Unit = {

		while (isRunning) {
			stockId = rand.nextInt(priceList.size)
			val curPrice =  priceList(stockId) + rand.nextGaussian() * 0.05
			priceList = priceList.updated(stockId, curPrice)
			val curTime = Calendar.getInstance.getTimeInMillis

			// 将数据源收集写入SourceContext
			ctx.collect(StockPrice("symbol_" + stockId.toString, curTime, curPrice))
			Thread.sleep(rand.nextInt(10))
		}


	}

	override def cancel(): Unit = {
		isRunning = false
	}


}



class StockPriceTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[StockPrice](Time.seconds(5)){
	override def extractTimestamp(element: StockPrice): Long = {
		element.timeStamp
	}
}