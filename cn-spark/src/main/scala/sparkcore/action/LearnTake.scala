package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnTake {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnTake").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(Array(2,5,4,6,8,3))

    val takeData: Array[Int] = dataRDD.take(3)

    takeData.foreach(print)

  }


}
