package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnMap {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("learnMap").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val flatMapRDD: RDD[List[Double]] = listRDD.map({
      case 3 => List(3.1, 3.2)

      //一对0
      case 2 => List()

      //一对一
      case x => List(x * 2)
    })

    flatMapRDD.collect().foreach(println)

    /**
      * 输出结果
      *
      * List(2.0)
      * List()
      * List(3.1, 3.2)
      * List(8.0)
      *
      */

  }

}
