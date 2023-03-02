package com.cyfqz.dstream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random
case class Access(time:Long,domain:String,flow:Long)

object NodeParallelSource{
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value = streamEnv.addSource(new MyNoneParallelSource)
    value.print
  }
}

class MyNoneParallelSource extends  SourceFunction[Access]{

  private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
    val domains =List("flink","hadoop","spark","kafka")
    val random = new Random()
    while (isRunning){
      val time =System.currentTimeMillis()
      val domain = domains(random.nextInt(10000))
      val flow = random.nextInt(1000)
      1.to(10).map( x =>ctx.collect(Access(time , domain , flow )))
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
