package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnGlom {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("learnGlom").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)


    val listRdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10,11),3)

    //将一个分区的数据放到一个数组里
    val glomRdd: RDD[Array[Int]] = listRdd.glom()

    glomRdd.collect().foreach(x=>{
      println(x.mkString(","))
    })

    //glomRdd.collect().foreach(println)

    /**
      * 打印结果
      * 多了的余数会放到会放到最后一个分区
      * 1,2,3
      * 4,5,6,7
      * 8,9,10,11
      *
      */



  }


}
