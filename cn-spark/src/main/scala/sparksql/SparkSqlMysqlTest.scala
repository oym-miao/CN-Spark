package sparksql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode

/**
 * @author Administrator
 */
object SparkSqlMysqlTest {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
              .builder()
              .master("local[*]")
              .appName("SparkSqlMysqlTest")
              .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")        
      val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.91.101:3306/db01")
      .option("dbtable", "TBLS")
      .option("user", "root")
      .option("password", "miao")
      .load()
      //jdbcDF.rdd.partitions.size:1
      println("jdbcDF.rdd.partitions.size:"+jdbcDF.rdd.partitions.size)
      jdbcDF.show()
      jdbcDF.printSchema()
      jdbcDF.explain()
      
      val connectionProperties = new Properties()
      connectionProperties.put("user", "root")
      connectionProperties.put("password", "miao")
      val jdbcDFPartition=spark.read.jdbc("jdbc:mysql://192.168.91.101:3306/db01", "TBLS", "TBL_ID", 1, 23, 3, connectionProperties)
      //jdbcDFPartition.rdd.partitions.size:3
      println("jdbcDFPartition.rdd.partitions.size:"+jdbcDFPartition.rdd.partitions.size)
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "123456")
//    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://bigdata03:3306/test", "employee", connectionProperties)
//    jdbcDF2.show()
    
    // Saving data to a JDBC source ，把上面表中读取到的数据加载到下表中
//    jdbcDF.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://bigdata03:3306/test")
//      .option("dbtable", "employee2")
//      .option("user", "root")
//      .option("password", "123456")
//      .mode(SaveMode.Append)
//      .save()

//    jdbcDF2.write.jdbc("jdbc:mysql://bigdata03:3306/test", "employee2", connectionProperties)
  
  }
}