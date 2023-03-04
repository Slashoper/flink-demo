package com.cyfqz.dstream.time

import com.cyfqz.dstream.transformation.StaionLog
import com.cyfqz.dstream.window.TestAggregateFunctionByWindow.MyAggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

// 无序的watermaker有周期性的还有间断性的
object MaxLongCallTime2 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    streamEnv.getConfig.setAutoWatermarkInterval(100L) //周期性watermarker时间间隔。默认100L

    val stream = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
        var arr = line.split(",")
        StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
      })  // 使用周期性
      // 1.直接采用AssignerWithPeriodicWatermakers 接口实现类(Flink提供的)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StaionLog](Time.seconds(3)) {
//        override def extractTimestamp(element: StaionLog): Long = {
//           element.callTime.toLong
//        }})
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StaionLog] {
        var maxEventTime:Long =_
        override def getCurrentWatermark: Watermark = {
           new Watermark(maxEventTime - 3000L)
        } // 周期性的生成watermaerk

        // 设定EventTime是哪个属性
        override def extractTimestamp(element: StaionLog, recordTimestamp: Long): Long = {
           maxEventTime = maxEventTime.max(element.callTime.toLong)
           element.callTime.toLong
        }
      })

    stream.map(log => (log.station, 1)).
      keyBy(_._1).window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(5)))
      .aggregate(new MyAggregateFunction)
    streamEnv.execute()
  }

}
