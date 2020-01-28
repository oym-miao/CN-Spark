package sparkcore.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnSubtract {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnSubtract").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[Int] = sc.parallelize(3 to 8)

    val rddTwo: RDD[Int] = sc.parallelize(1 to 5)

    val subractRddOne: Array[Int] = rddOne.subtract(rddTwo).collect()

    subractRddOne.foreach(println)

    /**
      * 输出结果
      * 即将两个rdd合并起来，第一个rdd中没有的数据，就是它的差集
      * 8
      * 6
      * 7
      */

    println("=====================")

    val subractRddTow: Array[Int] = rddTwo.subtract(rddOne).collect()

    subractRddTow.foreach(println)

    /**
      * 输出结果
      * 即将两个rdd合并起来，第二个rdd中没有的数据，就是它的差集
      * 1
      * 2
      */


  }
}
