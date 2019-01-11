package sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author brave
  *         集群运行命令：
  *         ./spark-submit --master spark://bigdata01:7077 --class com.brave.prepare.SparkSqlHiveTest /opt/testdata/sparkhiveTest.jar
  *
  */
object SparkSqlHiveTest {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {

    val warehouseLocation = "hdfs://oym2.com:8082/user/hive/warehouse"
    //  .config("spark.sql.warehouse.dir",warehouseLocation)
    // .config("hive.metastore.warehouse.dir",warehouseLocation)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Exampl")
      //  .config("hive.metastore.uris","thrift://oym2.com:9083")
      // .config("hive.metastore.warehouse.dir","hdfs://oym.com:8082/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.91.101:8082/user/hive/warehouse")
      // .config("hive.metastore.warehouse.dir","hdfs://192.168.91.101:8082/user/hive/warehouse")

      .enableHiveSupport() //启用对hive的支持
      .getOrCreate()
    // spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    sql("show tables").show()
    sql("show databases").show()
    sql("CREATE TABLE IF NOT EXISTS src9 (key INT, value STRING ) USING hive")
  sql("LOAD DATA INPATH 'hdfs://ns/testdata/resource/kv1.txt' INTO TABLE src9")

    //sql("select * from student05").show()
    // sql("select * from student06").show()


    //使用HiveQL语言查询
   // sql("SELECT * FROM src9").show()
    //sql("show tables").show()

    //spark.sql("show tables").show();


    //聚合操作
    // sql("SELECT COUNT(*) FROM src4").show()
    //    spark.sql("SELECT COUNT(*) FROM src2").show()
    //SQL查询的结果本身就是dataframe，并支持所有函数。
    /*    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    //    DataFrames中的行类型为Row，可以按序号访问每个列。
        val stringsDS = sqlDF.map {
          case Row(key: Int, value: String) =>("key:"+key+",value:"+value)
        }
        stringsDS.show()*/
    //
    /*    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i,"createDataFrame_"+i)))
    //    recordsDF.show()
        recordsDF.createOrReplaceTempView("src2")
        //临时表与hive中的表进行join。如果临时表名和hive中的表名重复，spark会使用临时表。
        println("&&&&&&&&&&&&&&&&&&&&&")
        sql("SELECT * FROM src2 r JOIN src2 s ON r.key = s.key").show()
        spark.sql("SELECT * FROM src2 r JOIN src2 s ON r.key = s.key").show()*/

    //
    //
    ////    创建一个由Hive管理的parquet格式表，使用HQL语法而不是Spark SQL语法
    /*    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
        val df=spark.table("src")
        df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
        println("hive_records")
        sql("SELECT * FROM hive_records").show()*/


  }
}