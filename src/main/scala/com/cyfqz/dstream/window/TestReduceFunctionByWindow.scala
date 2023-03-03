package com.cyfqz.dstream.window

import com.cyfqz.dstream.transformation.StaionLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


// 1、根据时间进行分窗 : timewindow 1. Tumbling window 2.sliding window 3.session window
// 2、根据数据的数量进行分窗 : count window 1.Tumbling window 2.sliding window
// 根据 是否keyby 来分为 keyed window / global window

// stream.keyby() // 是Keyed 类型数据集
// .window() 指定窗口分配器类型
// .trigger() 指定触发器类型
// .evictor 指定 evictor或者不指定
// .allowedLateness 指定是否延迟处理数据
// .sideOutputLateData 指定Output Lag
// .reduce/aggregate/fold/apply() 指定窗口计算函数 ProcessWindowFunction (sum.max)

/** 统计每隔5秒统计每个基站的日志数量 */
// 增量聚合函数
object TestReduceFunctionByWindow {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.socketTextStream("localhost", 8888).map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).trim,
        arr(5).trim)
    }).map(log => (log.station, 1)).
      keyBy(_._1).timeWindow(Time.seconds(5)).
      reduce((t1, t2) => (t1._1, t1._2 + t2._2)).print
    streamEnv.execute()
  }
}