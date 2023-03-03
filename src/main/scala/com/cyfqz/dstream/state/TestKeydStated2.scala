package com.cyfqz.dstream.state

import com.cyfqz.dstream.state.TestKeyedState.CallIntervalFunction
import com.cyfqz.dstream.transformation.StaionLog

object TestKeydStated2 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    //1。初始化流计算环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = streamEnv.socketTextStream("localhost", 8888).map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
    })

    stream.keyBy(_.callout).mapWithState[(String,Long),StaionLog]{
        case (in:StaionLog,None) => ((in.callout,0),Some(in))
        case (in:StaionLog,pre:Some[StaionLog]) =>{
            var internal = in.callTime.toLong - pre.get.callTime.toLong
            ((in.callout,internal),Some(in))
        }
    }.print()
    streamEnv.execute()
  }

}
