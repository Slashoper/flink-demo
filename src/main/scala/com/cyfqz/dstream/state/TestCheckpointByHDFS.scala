package com.cyfqz.dstream.state

import org.apache.flink.runtime.state.filesystem.{FsStateBackend, FsStateBackendFactory}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 开启checkpoint
 */
object TestCheckpointByHDFS {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkpoint
    streamEnv.enableCheckpointing(5000)// 每隔5s开启一次checkpoint
    streamEnv.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/checkpoint/cp1")) // 存放检查点数据
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 开启语义
    streamEnv.getCheckpointConfig.setCheckpointTimeout(5000) // 超时时间
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // 最大同时多少个checkpoint
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) // 检查点保存机智
    //2 . 导入隐士转换
    import org.apache.flink.streaming.api.scala._
    // 3.读取数据
    //    val stream = streamEnv.socketTextStream("locahost",7023)
    val stream = streamEnv.readTextFile("D:\\workspace\\code\\spark\\build\\spark-build-info")
    // 4.转换和处理数据
    var result = stream.flatMap(_.split("")).map((_, 1)).keyBy(0).sum(1)
    //5.打印结果
    result.print("结果")
    // 6.启动流计算程序
    streamEnv.execute("wordcount")
  }

}
