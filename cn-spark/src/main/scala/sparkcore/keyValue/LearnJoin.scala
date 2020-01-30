package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnJoin {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnJoin").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))

    val rddTwo: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))


    val joinOne: RDD[(Int, (String, Int))] = rddOne.join(rddTwo)

    joinOne.collect().foreach(print)

    /**
      * 输出结果
      * (1,(a,4))(2,(b,5))(3,(c,6))
      */

    val rddThree: RDD[(Int, Int)] = sc.parallelize(Array((1,9),(2,8),(3,7)))

    //将一个rdd跟第二个rdd join后，再将所得的结果跟第三个rdd进行join
    val joinTwo: RDD[(Int, ((String, Int), Int))] = rddOne.join(rddTwo).join(rddThree)

    joinTwo.collect().foreach(print)

    /**
      * 输出结果
      * (1,((a,4),9))(2,((b,5),8))(3,((c,6),7))
      */

  }

}
