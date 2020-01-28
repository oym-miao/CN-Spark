package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnRepartition {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnRepartition").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(1 to 16,4)

    dataRDD.collect().foreach(println)

    val repartitionRDD: RDD[Int] = dataRDD.repartition(2)

    //

    //println(repartitionRDD.glom().collect().mkString(","))

    val arrayRDD: Array[Array[Int]] = repartitionRDD.glom().collect()

    arrayRDD.foreach(println)

    /**
      * 输出结果，两个集合对象
      *
      * [I@de18f63
      * [I@108bdbd8
      */




  }


}
