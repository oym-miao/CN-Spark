package sparkcore.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnUnion {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnUnion").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[Int] = sc.makeRDD(1 to 5)

    val rddTwo: RDD[Int] = sc.makeRDD(5 to 10)

    val unionRdd: RDD[Int] = rddOne.union(rddTwo)

    val unionResultRdd: Array[Int] = unionRdd.collect()

    unionRdd.foreach(println)

    /**
      *
      * 输出结果
      *
      * 即使有重复的 也会合并进来 即不去重 ，如下5
      *
      * 3
      * 4
      * 1
      * 2
      * 5
      * 5
      * 6
      * 7
      * 8
      * 9
      * 10
      */



  }
}
