package com.cyfqz.dstream.source

import akka.io.Tcp.Connect
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

object RichParallelSourceFunction {
  def main(args: Array[String]): Unit = {

  }
}

case class City(id:Long,name:String,area:String)

class MysqlSource extends RichParallelSourceFunction[City]{

  private var conn:Connection = _
  private var state:PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    val url = "jdbc:mysql://localhost:3306"
    val user = "user"
    val password = "password"
    Class.forName("com.mysql.jdbc.driver")
    conn = DriverManager.getConnection(url,user,password)
  }


  override def run(ctx: SourceFunction.SourceContext[City]): Unit = {
      val sql = "select * from table_test"
      state = conn.prepareStatement(sql)
      val rs = state.executeQuery()
      while(rs.next()){
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val area = rs.getString(3)
        ctx.collect(City(id,name,area))
      }
  }

  override def cancel(): Unit = {

  }

  override def close(): Unit = super.close()

}
