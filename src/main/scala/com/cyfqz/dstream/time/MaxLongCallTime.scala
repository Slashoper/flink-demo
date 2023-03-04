package com.cyfqz.dstream.time

import com.cyfqz.dstream.transformation.StaionLog
import com.cyfqz.dstream.window.TestAggregateFunctionByWindow.MyAggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object MaxLongCallTime {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
    })
      .assignAscendingTimestamps(_.callTime.toLong) // 有序时间语义

      stream.map(log => (log.station, 1)).
      keyBy(_._1).window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(5)))
      .aggregate(new MyAggregateFunction)
    streamEnv.execute()
  }

}
