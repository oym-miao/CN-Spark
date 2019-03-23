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

    //foreachPartition这种方式性能更优写，不用每一条数据都创建一个链接，而是给一个partition创建一个链接
    mapRDD.foreachPartition(partitions =>{
      //1.遍历每一个partion,而每一个partion里面有多条记录，那么我每一个partition创建一个connection,而这个connection就可以往里面插入多条记录
      //2.一个partition是在一个task里面运行的，而一个task肯定是在一个excutor里面运行的，所以它是在一台机器上运行的，就不会出现网络传输
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
