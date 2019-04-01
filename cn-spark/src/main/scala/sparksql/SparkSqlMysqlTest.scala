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
      .option("url", "jdbc:mysql://oym2.com:3306/sqoop")
      .option("dbtable", "mysqltohdfs")
      .option("user", "root")
      .option("password", "miao")
      .load()
      //jdbcDF.rdd.partitions.size:1
      println("jdbcDF.rdd.partitions.size:"+jdbcDF.rdd.partitions.size)
      jdbcDF.show()
      jdbcDF.printSchema()
      jdbcDF.explain()

    /*
     //这里是进行读取mysql
     val connectionProperties = new Properties()
     connectionProperties.put("user", "root")
     connectionProperties.put("password", "miao")
     val jdbcDFPartition=spark.read.jdbc("jdbc:mysql://oym2.com:3306/db01", "TBLS", "TBL_ID", 1, 23, 3, connectionProperties)
     //jdbcDFPartition.rdd.partitions.size:3
     println("jdbcDFPartition.rdd.partitions.size:"+jdbcDFPartition.rdd.partitions.size)*/



    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "miao")
    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://oym2.com:3306/sqoop", "mysqltohdfs", connectionProperties)
    jdbcDF2.show()
    
   //  Saving data to a JDBC source ，把上面表中读取到的数据加载到下表中
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://oym2.com:3306/sqoop")
      .option("dbtable", "test2")
      .option("user", "root")
      .option("password", "miao")
      .mode(SaveMode.Append)
      .save()

    println("*********************")
    jdbcDF.show()
    jdbcDF.printSchema()
    //jdbcDF2.write.jdbc("jdbc:mysql://oym2.com:3306/sqoop", "test2", connectionProperties)
  
  }
}