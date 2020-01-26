package sparkcore.value

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object LearnFlatMap {

  def main(args: Array[String]): Unit = {

    val conf = new  SparkConf().setAppName("learnFlatMap").setMaster("local[*]")

    val sc = new SparkContext(conf)


    //val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1),List(2)))
    //val listRDD: RDD[Int] = sc.makeRDD(1 to 5)

    val listRDD: RDD[Int] = sc.parallelize(List(1,2,3,4))

    val value: RDD[Double] = listRDD.flatMap {

      //一对多  例如读一行数据需要对数据进行切分， 提取出这一行需要的数据, 或者将某个数据转换成多个
      case 3 => List(3.1, 3.2)

      //一对0
      case 2 => List()

      //一对一
      case x => List(x * 2)

    }

    value.collect().foreach(println)



    /**
      * 输出结果
      * 这个过程就像是先 map, 然后再将 map 出来的这些列表首尾相接 (flatten)
      * 2.0
      * 3.1
      * 3.2
      * 8.0
      */

  }

}
