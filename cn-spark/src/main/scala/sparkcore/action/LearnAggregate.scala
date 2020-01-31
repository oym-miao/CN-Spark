package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnAggregate {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnAggregate").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

    val aggreateData: Int = dataRDD.aggregate(0)(_+_,_+_)

    print(aggreateData)

    /**
      * 输出结果
      * 55
      */

    val dataRDDTwo: RDD[Int] = sc.makeRDD(1 to 10,2)
    //注意aggregate 分区间还会加一个10
    val aggreateDataTwo: Int = dataRDDTwo.aggregate(10)(_+_,_+_)

    print(aggreateDataTwo)
    /**
      * 输出结果
      * 85
      */


  }

}
