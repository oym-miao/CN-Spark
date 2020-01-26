package sparkcore.value

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object LearnFlatMap {

  def main(args: Array[String]): Unit = {

    val conf = new  SparkConf().setAppName("learnFlatMap").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val list = List(1,2,3,4)

    val listRDD: List[Double] = list.flatMap(x => x match {

      //一对多
      case 3 => List(3.1, 3.2)

      //一对0
      case 2 => List()

      //一对一
      case _ => List(x * 2)

    })


    println(listRDD)

    /**
      * 输出结果
      * 这个过程就像是先 map, 然后再将 map 出来的这些列表首尾相接 (flatten)
      * List(2, 3.1, 3.2, 8)
      */

  }

}
