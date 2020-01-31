package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnFirst {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnFirst").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(1 to 10)

    val firstData: Int = dataRDD.first()

    print(firstData)

    /**
      * 输出结果
      * 1
      */



  }
}
