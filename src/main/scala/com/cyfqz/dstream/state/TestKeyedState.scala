package com.cyfqz.dstream.state

import com.cyfqz.dstream.transformation.StaionLog
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * state
 *
 *  统计每个手机的时间间隔
 */
object TestKeyedState {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    //1。初始化流计算环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = streamEnv.socketTextStream("localhost", 8888).map(line => {
      var arr = line.split(",")
      StaionLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim)
    })

    stream.keyBy(_.callout).flatMap(new CallIntervalFunction)

  }
  // 富函数类可以使用状态
  class CallIntervalFunction extends RichFlatMapFunction[StaionLog,(String,Long)]{
    private var preCallTimeState:ValueState[Long]=_


    override def open(parameters: Configuration): Unit = {

      preCallTimeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("pre",classOf[Long]))

    }


    override def flatMap(value: StaionLog, out: Collector[(String, Long)]): Unit = {
      // 从状态中取出前一次状态的值
      var preCallTime = preCallTimeState.value()
      if(preCallTime == null || preCallTime == 0){
        preCallTimeState.update(value.callTime.toLong)
      }else{ // 状态中有数据，
        var interval = value.callTime.toLong - preCallTime.toLong
        out.collect((value.callout,interval)) // 输出手机号，时间间隔
      }
    }

    override def close(): Unit = super.close()

  }
}
