package com.cyfqz.dstream.time

import com.cyfqz.dstream.transformation.StaionLog
import com.cyfqz.dstream.window.TestAggregateFunctionByWindow.MyAggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object LatenessDataOnWindow {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
        var arr = line.split(",")
        StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
      }) // 使用Flink默认的周期watermaker类
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[StaionLog](Time.seconds(2)) {
        override def extractTimestamp(element: StaionLog): Long = {
          element.callTime.toLong
        }
      })

    var lateTag = new OutputTag[(String,Int)]("late")

    stream.map(
      log => (log.station, 1))
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(5)))
      // 设置迟到超出了2秒的情况下，怎么办，交给allowedLateness处理
      // 也分两种情况，第一种：允许数据迟到5秒(迟到2-5秒)，再次延吃触发窗口函数，触发条件：watermark < end-of-window + allowedlanteness
      .allowedLateness(Time.seconds(5))
      // 超出5秒的数据，定义测流的标签
      .sideOutputLateData(lateTag)
      .aggregate(new MyAggregateFunction)

       streamEnv.execute()
  }

  class OutPutResultWindowFunction extends WindowFunction[Long,String,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
      var value = input.iterator.next()
      var sb = new StringBuilder()
      sb.append("窗口范围：").append(window.getStart).append("-----").append(window.getEnd)
    }
  }
}
