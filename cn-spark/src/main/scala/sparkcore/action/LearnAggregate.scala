package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnAggregate {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnAggregate").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(1 to 3,2)

    val aggreateData: Int = dataRDD.aggregate(0)(_+_,_+_)

    print(aggreateData)

    /**
      * 输出结果
      * 6
      */

  }

}
