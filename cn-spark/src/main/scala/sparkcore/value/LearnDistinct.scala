package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnDistinct {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnDistinct").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,4,5,6,7,8))

    //使用distinct算子对数据进行去重，但是因为去重后会导致数据减少，所以可以改变默认的分区数量
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    distinctRDD.saveAsTextFile("output")


  }


}
