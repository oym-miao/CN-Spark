package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnSample {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnSample").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,6,5,7,8,9,10))

    //参数1：是否需要重新放回ture表示放回，false表示不放回  参数2: 抽取的比例(不一定精确),例如这里是抽取40%的数据
    val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = false,0.4,1)

    sampleRDD.collect().foreach(println)

    /**
      * 输出结果
      * 2
      * 3
      * 4
      * 10
      */

    println("==========================")

    //取指定数量的随机数据
    val takeSampleList: Array[Int] = listRDD.takeSample(withReplacement = false,2,4)
    takeSampleList.foreach(println)

    /**
      * 输出结果
      * 6
      * 8
      *
      */

    println("================")


    //取排序好的指定数量的数据
    val takeOrderRdd: Array[Int] = listRDD.takeOrdered(8)
    takeOrderRdd.foreach(println)

    /**
      * 输出结果
      * 1
      * 2
      * 3
      * 4
      * 5
      * 6
      * 7
      * 8
      */



  }


}
