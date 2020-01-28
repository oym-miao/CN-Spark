package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnSortBy {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnSortBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(2,1,3,4))

    val sortRDD: RDD[Int] = dataRDD.sortBy(x=>x)

    sortRDD.collect().foreach(println)

    /**
      * 输出结果
      *
      * 1
      * 2
      * 3
      * 4
      */









  }


}
