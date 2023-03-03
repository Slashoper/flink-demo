package com.cyfqz.dstream.checkpoint

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

// checkpoint
object TestCheckPointByHdfs {

  def main(args: Array[String]): Unit = {
      import org.apache.flink.api.scala._
      val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
      // 多久checkponit
      streamEnv.enableCheckpointing(5000)

  }

}
