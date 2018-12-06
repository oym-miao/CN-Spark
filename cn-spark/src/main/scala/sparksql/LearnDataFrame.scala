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
    //导入隐式转换工具包
    import spark.implicits._

//    val seq=Seq(
//      ("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
//      ("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
//    )
//    //toDF隐式转换工具包
//    val df=seq.toDF("name","age","address","birthday")
//    df.show()
//    df.printSchema()
//
//    val personSeq=Seq(
//      Person("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
//      Person("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
//    )
//
//    val personDF=personSeq.toDF()
//    personDF.show()
//    personDF.printSchema()
//    println("*******************")
//    val rdd=spark.sparkContext.parallelize(seq)
//    rdd.toDF().show()
//    println("_____________________")
//    personDF.createOrReplaceTempView("person")
//    spark.sql("select * from person where age>10").show()

//    val employeeDF=spark.read.json("D:\\idea2018workspace\\cnw_spark_svn\\in\\people.json")
//    employeeDF.show()
//    employeeDF.printSchema()
//    employeeDF.createOrReplaceTempView("employee")

    import org.apache.spark.sql.functions.{col, column}
    import org.apache.spark.sql.functions.expr
    val df = spark.read.format("json") .load("in/2015-summary.json")
//    df.printSchema()
//    df.select(col("DEST_COUNTRY_NAME")).show();
//    df.select("DEST_COUNTRY_NAME").show()
//
//    df.createOrReplaceTempView("flight")
//    spark.sql("SELECT  DEST_COUNTRY_NAME  FROM  flight LIMIT 2").show()
//
//    df.select("DEST_COUNTRY_NAME","DEST_COUNTRY_NAME")
//
//
//    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
//    spark.sql("SELECT DEST_COUNTRY_NAME as destination FROM flight").show(2)
//
//    import org.apache.spark.sql.functions.lit
//    df.select(expr("*"), lit(1).as("One")).show(2)
//    spark.sql("SELECT *, 1 as One FROM flight LIMIT 2")

//    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show()
    val df2=df.withColumnRenamed("DEST_COUNTRY_NAME", "dest");
    df2.printSchema()
  }
}
