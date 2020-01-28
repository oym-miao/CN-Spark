package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnReduceByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnReduceByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //创建pairRDD
    val pairRDD: RDD[(String, Int)] = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))

    //计算相同key对应值的相加结果
    val reducePairRDD: RDD[(String, Int)] = pairRDD.reduceByKey((x,y)=>x+y)

    reducePairRDD.collect().foreach(print)

    /**
      * 输出结果
      * (female,6)(male,7)
      */


  }


}
