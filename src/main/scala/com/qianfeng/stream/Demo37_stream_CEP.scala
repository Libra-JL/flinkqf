package com.qianfeng.stream

import com.qianfeng.common.{Event, SubEvent}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.util.Collector


object Demo37_stream_CEP {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val ds = env.fromElements(
			new Event(1, "start", 1.0),
			new Event(2, "middle", 2.0),
			new Event(3, "foobar", 3.0),
			new Event(4, "foo", 4.0),
			new Event(5, "middle", 5.0),
			new SubEvent(6, "middle", 6.0, 100),
			new SubEvent(7, "bar", 7.0, 1000),
			new Event(66, "666", 66.0),
			new Event(8, "end", 8.9)
		)

		val pattern: Pattern[Event, Event] = Pattern.begin[Event]("start")
			.where(_.getName.equals("start"))
			.followedByAny("middle")
			.where(_.getName.equals("middle"))
			.followedByAny("end")
			.where(_.getName.equals("end"))

		val ps = CEP.pattern(ds, pattern)

		ps.flatSelect((ele:scala.collection.Map[String,Iterable[Event]],out:Collector[String])=>{
			val builder = new StringBuilder

			val startEvent = ele.get("start").get.toList.head.toString
			val middleEvent = ele.get("middle").get.toList.head.toString
			val endEvent = ele.get("end").get.toList.head.toString


			builder.append(startEvent)
    			.append("\t|\t")
    			.append(middleEvent)
    			.append("\t|\t")
    			.append(endEvent)

			out.collect(builder.toString())

		}).print("cep")

		env.execute("cep")

	}
}
