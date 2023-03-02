package com.cyfqz.dstream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

object ParallelSource {
  private var isRunning = true
  def main(args: Array[String]): Unit = {

  }
}

class MyParallelSource extends ParallelSourceFunction[Access] {
  private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
    val domains = List("flink.com","spark.com","mp.csdn.net")
    val random = new Random()
    while(isRunning){
      val time = System.currentTimeMillis() + ""
      val domain = domains(random.nextInt(domains.length))
      val flow = random.nextInt(1000)
      1.to(10)
    }
  }


  override def cancel(): Unit = {

  }
}
