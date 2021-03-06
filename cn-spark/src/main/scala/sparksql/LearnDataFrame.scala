package sparksql

import java.sql.Date

import org.apache.spark.sql.SparkSession

object LearnDataFrame {
  case class Person(name:String,age:Int,address:String,birthday:Date)
  def main(args: Array[String]): Unit = {
    /**
      *builder() 建造者模式
      */
    val spark=SparkSession.builder().appName("LearnDataFrame").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    /**
      * import spark.implicits._
      * 导入隐式转换函数，可以将本地序列(seq), 列表或者RDD转为DataFrame
      * 可以使用toDF("name","age","address","birthday")指定列名
      * 如果不指定列名，spark默认设置列名为：_1,_2,_3,_4
      */
    //这个集合里面有两个tuple
    val seq=Seq(
      ("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
      ("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
    )
    //toDF隐式转换工具包
    val df=seq.toDF("name","age","address","birthday")
    df.show()
    df.printSchema()

    /*
    上面打印结果如下
    +--------+---+--------+----------+
    |    name|age| address|  birthday|
    +--------+---+--------+----------+
    |zhangsan| 10| beijing|2008-01-01|
    |    lisi| 20|shanghai|1998-01-01|
    +--------+---+--------+----------+

    root
    |-- name: string (nullable = true)
    |-- age: integer (nullable = false)
    |-- address: string (nullable = true)
    |-- birthday: date (nullable = true)
    */


//
//    val personSeq=Seq(
//      Person("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
//      Person("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
//    )
//    //注意这里toDF的时候就不需要指定字段了
//    val personDF=personSeq.toDF()
//    personDF.show()
//    personDF.printSchema()
//    println("*******************")
      //这种是rdd的转换成DF
//    val rdd=spark.sparkContext.parallelize(seq)
//    rdd.toDF().show()
//    println("_____________________")
//    personDF.createOrReplaceTempView("person")
//    spark.sql("select * from person where age>10").show()




//    val employeeDF=spark.read.json("D:\\cainiaolearn\\spark\\learnSpark\\CN-Spark\\cn-spark\\in\\people.json")
//    employeeDF.show()
//    employeeDF.printSchema()
//    employeeDF.createOrReplaceTempView("employee")




/*    import org.apache.spark.sql.functions.{col, column}
    import org.apache.spark.sql.functions.expr
    val df = spark.read.format("json") .load("in/2015-summary.json")
    df.printSchema()
    df.select(col("DEST_COUNTRY_NAME")).show();
    df.select("DEST_COUNTRY_NAME").show()

    df.createOrReplaceTempView("flight")
    spark.sql("SELECT  DEST_COUNTRY_NAME  FROM  flight LIMIT 2").show()

    df.select("DEST_COUNTRY_NAME","DEST_COUNTRY_NAME")


    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
    spark.sql("SELECT DEST_COUNTRY_NAME as destination FROM flight").show(2)*/



      //in scala
//    import org.apache.spark.sql.functions.lit
//    df.select(expr("*"), lit(1).as("One")).show(2)
    //in sql
//    spark.sql("SELECT *, 1 as One FROM flight LIMIT 2")

      //新增一列 withinCountry 而这个列的值是根据两个国家的名字是否相等来返回的
//    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show()

    //  val df2=df.withColumnRenamed("DEST_COUNTRY_NAME", "dest");
   // df2.printSchema()
  }
}
