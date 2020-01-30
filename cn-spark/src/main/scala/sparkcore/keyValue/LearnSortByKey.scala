package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnSortByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnSortByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRdd: RDD[(Int, String)] = sc.makeRDD(List((1,"张三"),(2,"李四"),(4,"赵钱"),(4,"赵钱"),(3,"王五"),(5,"孙李")))

    val sortByKeyRDD: RDD[(Int, String)] = dataRdd.sortByKey(true)

    sortByKeyRDD.collect().foreach(print)

    /**
      * 输出结果
      * (1,张三)(2,李四)(3,王五)(4,赵钱)(4,赵钱)(5,孙李)
      */


  }

}
