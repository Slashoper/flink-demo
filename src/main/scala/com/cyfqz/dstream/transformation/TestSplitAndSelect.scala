package com.cyfqz.dstream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestSplitAndSelect {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val stream = streamEnv.readTextFile("d://data")
    stream.split( log => {

    })
  }

}
