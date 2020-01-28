package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCoalesce {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCoalesce").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD: RDD[Range.Inclusive] = sc.makeRDD(List(1 to 16),4)

    println("缩减分区前的分区数= "+listRDD.partitions.size)

    //缩减分区数，可以简单的理解为合并分区, 不会产生shufful
    val coalesceRDD: RDD[Range.Inclusive] = listRDD.coalesce(3)
    println("缩减分区后= "+coalesceRDD.partitions.size)

    coalesceRDD.saveAsTextFile("output")


  }

}
