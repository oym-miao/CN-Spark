package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnReduce {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnReduce").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(1 to 3,2)

    val reduceRDDOne: Int = dataRDD.reduce(_+_)

    println(reduceRDDOne)

    /**
      * 输出结果
      * 6
      */

    val dataRDDTwo: RDD[(String, Int)] = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))

    val reduceTwo: (String, Int) = dataRDDTwo.reduce((x,y)=>(x._1+y._1,x._2+y._2))

    print(reduceTwo)

    /**
      * 输出结果
      * (caad,12)
      */

  }


}
