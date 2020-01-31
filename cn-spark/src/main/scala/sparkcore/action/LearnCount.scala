package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(1 to 10)

    val count: Long = dataRDD.count()

    print(count)

    /**
      * 输出结果
      * 10
      */

    val pairRDD: RDD[(String, Int)] = sc.parallelize(List(("a",1),("b",2),("c",3)))

    val pairRDDCount: Long = pairRDD.count()

    print(pairRDDCount)

    /**
      * 输出结果
      * 3
      */


  }


}
