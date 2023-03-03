package com.cyfqz.dstream.window

import com.cyfqz.dstream.transformation.StaionLog
import com.cyfqz.dstream.window.TestAggregateFunctionByWindow.MyAggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 全量窗口函数
// 统计5秒钟的窗口数据
object TestProcessWindowsFunction {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.socketTextStream("localhost", 8888).map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
    }).map(log => (log.station, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow]{
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          out.collect(key,elements.size)
        }
      })
  }

}
