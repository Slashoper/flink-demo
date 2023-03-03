package com.cyfqz.dstream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}

object CustomerJdbcSink {

  //需求：随机生成station
  def main(args: Array[String]): Unit = {

  }

  // 自定义 sink (可以管理生命周期的函数)
  class MyCustomerSink extends RichSinkFunction[String]{
     var conn:Connection= _
     var pst:PreparedStatement=_


    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://localhost/test","root","123")
        pst = conn.prepareStatement("insert into t_station_log()")
    }

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
        pst.setString(1,value)
        pst.execute()
    }

    override def close(): Unit = {
        conn.close()
        pst.close()
    }
  }
}
