package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnAggregateByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnAggregateByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    dataRDD.glom().collect().foreach(x=>{
      //println(x.foreach(print))
      x.foreach(print)
      println("======")
    })

    /**
      * 输出结果
      * (a,3)(a,2)(c,4)=====
      * (b,3)(c,6)(c,8)=====
      */


    println("=================")
    //需求：创建一个pairRDD，取出每个分区相同key对应值的最大值(计算每个分区内的)，然后相加(计算分区间的)
    val aggregateByKeyRdd: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(Math.max(_,_),(_+_))

    aggregateByKeyRdd.collect().foreach(print)


  }


}
