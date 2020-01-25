package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnMapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LearnMapPartitionsWithIndex")

    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(1 to 10)

    //需求: 创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
    val tupleRDD: RDD[(Int,String)] = listRDD.mapPartitionsWithIndex {
      //case() => {}   这个表示函数的入口
      case (num, datas) => {
        datas.map((_, "分区号: " + num))
      }
    }

    tupleRDD.collect().foreach(println)


    /**
      * 打印结果
      *
      * (1,分区号: 0)
      * (2,分区号: 1)
      * (3,分区号: 2)
      * (4,分区号: 3)
      * (5,分区号: 3)
      * (6,分区号: 4)
      * (7,分区号: 5)
      * (8,分区号: 6)
      * (9,分区号: 7)
      * (10,分区号: 7)
      */




  }

}
