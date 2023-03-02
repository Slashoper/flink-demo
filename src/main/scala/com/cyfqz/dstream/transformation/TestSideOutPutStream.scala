package com.cyfqz.dstream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 把主流进行分流
 */
object TestSideOutPutStream {

  import org.apache.flink.streaming.api.scala._
  var notSuccessTag = new OutputTag[StaionLog]("NOT_SUCCESS") // 不成功的测流标签
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.streaming.api.scala._
    val filePath = getClass.getResource("/station.log").getPath;
    // 读取数据源
    val stream: DataStream[StaionLog] = streamEnv.readTextFile(filePath).map(line => {
      var arr = line.split(",")
      new StaionLog(arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).trim,
        arr(5).trim)
    });

    val result: DataStream[StaionLog] = stream.process(new CreateSideOutputStream(notSuccessTag))
    result.print("主流")
    // 一定要根据主流来判断
    val sideOutputStream:DataStream[StaionLog] = result.getSideOutput(notSuccessTag)
    sideOutputStream.print("测流")

  }


  class CreateSideOutputStream(tag:OutputTag[StaionLog]) extends ProcessFunction[StaionLog,StaionLog]{
    override def processElement(value: StaionLog, ctx: ProcessFunction[StaionLog, StaionLog]#Context, out: Collector[StaionLog]): Unit = {
      if(value.callType.equals("success")){
        out.collect(value) //输出主流
      }else{ // 输出侧流
        ctx.output(notSuccessTag,value)
      }
    }
  }
}


