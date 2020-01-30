package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnMapValues {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnMapValues").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    //将value添加字符串"|||"
    val mapValueRDD: RDD[(Int, String)] = dataRDD.mapValues(_+"||")

    mapValueRDD.collect().foreach(print)

    /**
      * 输出结果
      * (1,a||)(1,d||)(2,b||)(3,c||)
      */

  }

}
