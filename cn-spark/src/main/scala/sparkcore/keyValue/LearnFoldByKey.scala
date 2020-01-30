package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnFoldByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnFoldByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val foldRDD: RDD[(Int, Int)] = dataRDD.foldByKey(0)(_+_)

    foldRDD.collect().foreach(print)

    /**
      * 输出结果
      * (3,14)(1,9)(2,3)
      */


  }
}
