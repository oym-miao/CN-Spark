package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCollect {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCollect").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val data: Array[Int] = dataRDD.collect()

    data.foreach(print)

    /**
      * 输出结果
      * 12345678910
      */

  }

}
