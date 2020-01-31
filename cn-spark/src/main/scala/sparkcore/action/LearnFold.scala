package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnFold {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnFold").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(1 to 3,2)

    val foldData: Int = dataRDD.fold(0)(_+_)

    print(foldData)

    /**
      * 输出结果
      * 6
      */

  }

}
