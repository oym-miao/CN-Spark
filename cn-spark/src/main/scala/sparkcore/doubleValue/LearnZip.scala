package sparkcore.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnZip {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnZip").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[Int] = sc.parallelize(Array(1,2,3),3)

    val rddTwo: RDD[String] = sc.parallelize(Array("a","b","c"),3)

    val rddThree: RDD[String] = sc.parallelize(Array("a","b","c"),2)

    //zip一
    val zipOne: RDD[(Int, String)] = rddOne.zip(rddTwo)
    zipOne.collect().foreach(println)

    /**
      * 打印输出
      *
      * (1,a)
      * (2,b)
      * (3,c)
      */

    println("=================")

    //zip二
    val zipTwo: RDD[(String, Int)] = rddTwo.zip(rddOne)
    zipTwo.collect().foreach(println)

    /**
      * 打印输出
      * (a,1)
      * (b,2)
      * (c,3)
      */


    //zip三 这里是测试一样，当分区数不一致 会报如下错误
    // java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(3, 2)
    val zipThree: RDD[((Int, String), String)] = zipOne.zip(rddThree)
    zipThree.collect().foreach(println)


  }
}
