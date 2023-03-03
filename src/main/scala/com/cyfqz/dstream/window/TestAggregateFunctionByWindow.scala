package com.cyfqz.dstream.window

import com.cyfqz.dstream.transformation.StaionLog
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction


// 每隔3秒计算5秒的数据
// 增量窗口函数
object TestAggregateFunctionByWindow {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.socketTextStream("localhost", 8888).map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
    }).map(log => (log.station, 1)).
      keyBy(_._1).window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(5)))
      .aggregate(new MyAggregateFunction)
    streamEnv.execute()
  }

  class MyAggregateFunction extends AggregateFunction[(String,Int),Long,Long] {

    override def createAccumulator(): Long = 0

    override def add(value: (String, Int), accumulator: Long): Long = accumulator + value._2 //定义累加器的相加

    override def getResult(accumulator: Long): Long = accumulator // 定义累加器的返回值

    override def merge(a: Long, b: Long): Long = a+b // 定义合并逻辑
  }
}
