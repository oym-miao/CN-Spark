package sparkcore.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnIntersection {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnIntersection").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[Int] = sc.parallelize(1 to 7)

    val rddTwo: RDD[Int] = sc.parallelize(5 to 10)

    val intersectionRdd: Array[Int] = rddOne.intersection(rddTwo).collect()

    intersectionRdd.foreach(println)

    /**
      * 输出结果
      *
      * 5
      * 6
      * 7
      */



  }


}
