package sparkcore.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCartesian {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCartesian").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[Int] = sc.parallelize(1 to 3)

    val rddTwo: RDD[Int] = sc.parallelize(2 to 5)

    val tuples: Array[(Int, Int)] = rddOne.cartesian(rddTwo).collect()

    tuples.foreach(println)

    /**
      * 输出结果
      *
      * (1,2)
      * (1,3)
      * (1,4)
      * (1,5)
      * (2,2)
      * (2,3)
      * (2,4)
      * (2,5)
      * (3,2)
      * (3,3)
      * (3,4)
      * (3,5)
      */



  }


}
