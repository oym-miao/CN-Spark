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
  *
  *        总结运行当前这个类碰到的问题：
  *             1.当前active namenode是在哪个节点，那么hive-site(连接hdfs的元数据配置),core-site(fs.default.name,不过这个配置不配置也行)中的地址就应该连接哪个节点
  *             2.hive-site中连接mysql的地址必须是启动了mysql的那个节点
  *             3.主节点中启动元数据服务, bin/hive --service metastore &
  *             4.通过spark读取hdfs中的数据到hive里面时，地址换成这个就行了hdfs://ns，
  *               如这个: sql("LOAD DATA INPATH 'hdfs://ns/testdata/resource/kv1.txt' INTO TABLE src10")
  *
  *
  *
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
     // .config("spark.sql.warehouse.dir", "hdfs://192.168.91.100:8082/user/hive/warehouse")
    .config("spark.sql.warehouse.dir", "hdfs://oym2.com:8082/user/hive/warehouse")
      // .config("hive.metastore.warehouse.dir","hdfs://192.168.91.101:8082/user/hive/warehouse")

      .enableHiveSupport() //启用对hive的支持
      .getOrCreate()
    // spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

     sql("show tables").show()
     sql("show databases").show()
     //sql("CREATE TABLE IF NOT EXISTS src12(key INT, value STRING ) USING hive")
    //sql("LOAD DATA INPATH 'hdfs://ns/testdata/kv1.txt' INTO TABLE src12")
    //  sql("LOAD DATA INPATH 'hdfs://oym2.com:8082/testdata/kv1.txt' INTO TABLE src12")

    //sql("select * from student05").show()
     // sql("select * from student06").show()
//    spark.sql("show tables").show()

    //使用HiveQL语言查询
  // sql("SELECT * FROM src12").show()
  // sql("show tables").show()

/*
      +---+-------+
      |key|  value|
      +---+-------+
      |238|val_238|
      | 86| val_86|
      |311|val_311|
      | 27| val_27|
      |165|val_165|
      |409|val_409|
      |255|val_255|
      |278|val_278|
      | 98| val_98|
      |484|val_484|
      |265|val_265|
      |193|val_193|
      |401|val_401|
      |150|val_150|
      |273|val_273|
      |224|val_224|
      |369|val_369|
      | 66| val_66|
      |128|val_128|
      |213|val_213|
      +---+-------+
    only showing top 20 rows
*/





    //spark.sql("show tables").show();


    //聚合操作
   // sql("SELECT COUNT(*) FROM src9").show()
    //    spark.sql("SELECT COUNT(*) FROM src2").show()
  /*  +--------+
    |  count (1) |
      +--------+
    |
         500     |
      +--------+*/



  /*

       //SQL查询的结果本身就是dataframe，并支持所有函数。
        val sqlDF = sql("SELECT key, value FROM src9 WHERE key < 10 ORDER BY key")
        //DataFrames中的行类型为Row，可以按序号访问每个列。
        val stringsDS = sqlDF.map {
          case Row(key: Int, value: String) =>("key:"+key+",value:"+value)
        }
        stringsDS.show()
*/
/*
      +-----------------+
      |            value|
      +-----------------+
      |key:0,value:val_0|
      |key:0,value:val_0|
      |key:0,value:val_0|
      |key:2,value:val_2|
      |key:4,value:val_4|
      |key:5,value:val_5|
      |key:5,value:val_5|
      |key:5,value:val_5|
      |key:8,value:val_8|
      |key:9,value:val_9|
      +-----------------+

*/



/*
     val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i,"createDataFrame_"+i)))
    //    recordsDF.show()
        recordsDF.createOrReplaceTempView("src9")
        //临时表与hive中的表进行join。如果临时表名和hive中的表名重复，spark会使用临时表。
        println("&&&&&&&&&&&&&&&&&&&&&")
        sql("SELECT * FROM src9 r JOIN src9 s ON r.key = s.key").show()
        spark.sql("SELECT * FROM src9 r JOIN src9 s ON r.key = s.key").show()
*/


/*
      //运行结果证明确实spark用的是临时表，而而不是顶部创建的那个表
      +---+------------------+---+------------------+
      |key|             value|key|             value|
      +---+------------------+---+------------------+
      |  1| createDataFrame_1|  1| createDataFrame_1|
      |  2| createDataFrame_2|  2| createDataFrame_2|
      |  3| createDataFrame_3|  3| createDataFrame_3|
      |  4| createDataFrame_4|  4| createDataFrame_4|
      |  5| createDataFrame_5|  5| createDataFrame_5|
      |  6| createDataFrame_6|  6| createDataFrame_6|
      |  7| createDataFrame_7|  7| createDataFrame_7|
      |  8| createDataFrame_8|  8| createDataFrame_8|
      |  9| createDataFrame_9|  9| createDataFrame_9|
      | 10|createDataFrame_10| 10|createDataFrame_10|
      | 11|createDataFrame_11| 11|createDataFrame_11|
      | 12|createDataFrame_12| 12|createDataFrame_12|
      | 13|createDataFrame_13| 13|createDataFrame_13|
      | 14|createDataFrame_14| 14|createDataFrame_14|
      | 15|createDataFrame_15| 15|createDataFrame_15|
      | 16|createDataFrame_16| 16|createDataFrame_16|
      | 17|createDataFrame_17| 17|createDataFrame_17|
      | 18|createDataFrame_18| 18|createDataFrame_18|
      | 19|createDataFrame_19| 19|createDataFrame_19|
      | 20|createDataFrame_20| 20|createDataFrame_20|
      +---+------------------+---+------------------+
    only showing top 20 rows

*/



/*

    ////    创建一个由Hive管理的parquet格式表，使用HQL语法而不是Spark SQL语法
     sql("CREATE TABLE IF NOT EXISTS hive_records(key int, value string) STORED AS PARQUET")
        val df=spark.table("src10")
       //将src表中的数据以及字段原封不动的加载到hive_records表中,如果hive_records这个表不存在的话，会创建这个表,SaveMode决定你是要覆盖这个表里面的数据，还是追加，还是覆盖，还是报错
        df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
        println("hive_records")
        sql("SELECT * FROM hive_records").show()
*/

/*
      +---+-------+
      |key|  value|
      +---+-------+
      |238|val_238|
      | 86| val_86|
      |311|val_311|
      | 27| val_27|
      |165|val_165|
      |409|val_409|
      |255|val_255|
      |278|val_278|
      | 98| val_98|
      |484|val_484|
      |265|val_265|
      |193|val_193|
      |401|val_401|
      |150|val_150|
      |273|val_273|
      |224|val_224|
      |369|val_369|
      | 66| val_66|
      |128|val_128|
      |213|val_213|
      +---+-------+
    only showing top 20 rows


*/

  }
}