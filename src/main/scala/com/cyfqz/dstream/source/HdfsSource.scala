package com.cyfqz.dstream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object HdfsSource {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //2 . 导入隐士转换
    import org.apache.flink.streaming.api.scala._
    // 3.读取数据
    //    val stream = streamEnv.socketTextStream("locahost",7023)
    val stream = streamEnv.readTextFile("hdfs://hadoop01:9000/wc.txt")
    // 4.转换和处理数据
    var result = stream.flatMap(_.split("")).map((_, 1)).keyBy(0).sum(1)
    //5.打印结果
    result.print("结果")
    // 6.启动流计算程序
    streamEnv.execute("wordcount")
  }

}
