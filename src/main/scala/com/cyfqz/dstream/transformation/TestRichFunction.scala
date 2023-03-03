package com.cyfqz.dstream.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class StaionLog(station:String,var callout:String,var callInt:String,callType:String,callTime:String,callLength:String)

object TestRichFunction {

  /**
   * 把通话成功的号码转换成真是的用户姓名
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.streaming.api.scala._
    val filePath = getClass.getResource("/station.log").getPath;
    // 读取数据源
    val stream: DataStream[StaionLog] = streamEnv.readTextFile(filePath).map(line => {
      var arr = line.split(",")
        StaionLog(arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).trim,
        arr(5).trim)
    });

    stream.filter(_.callType.equals("success")).map(k => new MyRichMapFunction)


  }

  class MyRichMapFunction extends RichMapFunction{
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
    }


    override def map(value: StaionLog): StaionLog = {
      StaionLog("001","0020","0002","0","23849283948293","8984928")
    }

    override def close(): Unit = {

    }

  }

}
