package sparkcore

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.JdbcUtils

/**
  *使用package打包，provided不会打包进去
  * ./spark-submit --class sparkcore.LearnSparkMySql --master spark://bigdata01:7077 /opt/sparkapp/spark-1.0-SNAPSHOT-jar-with-dependencies.jar
  */
object LearnSparkMySql {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
//    conf.setMaster("local[*]")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("hdfs://miao.com:9000/sparkdata5")
    val mapRDD=textFileRDD.map(line => line.size)
    //第一种方式
    mapRDD.foreach(lineSize => {
      val conn =JdbcUtils.getConnection()
      val sql = "insert into textFile (value) values(?)"
      var pstmt :PreparedStatement= null
      pstmt = conn.prepareStatement(sql)
      pstmt.setString(1,lineSize+"")
      pstmt.executeUpdate
      JdbcUtils.free(pstmt, conn)
    })

    mapRDD.foreachPartition(partitions =>{

      val conn =JdbcUtils.getConnection()
      var pstmt :PreparedStatement= null
      partitions.foreach(lineSize =>{
        val sql = "insert into textFile (value) values(?)"
        pstmt = conn.prepareStatement(sql)
        pstmt.setString(1,lineSize+"")
        pstmt.executeUpdate

      })
      JdbcUtils.free(pstmt, conn)
    })

  }

}
