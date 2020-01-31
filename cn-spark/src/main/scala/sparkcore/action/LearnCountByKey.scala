package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCountByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCountByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val countData: collection.Map[Int, Long] = dataRDD.countByKey()

    countData.foreach(print)

    /**
      * 输出结果
      * (3,2)(1,3)(2,1)
      */

  }

}
