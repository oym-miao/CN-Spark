package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCogroup {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCogroup").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rddOne: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val rddTwo: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6),(4,6)))

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rddOne.cogroup(rddTwo)

    cogroupRDD.collect().foreach(print)

    /**
      *输出结果
      * 注意 如果一个rdd 有这个key而另外一个rdd没有，那么没有的那个会显示为空
      * (1,(CompactBuffer(a),CompactBuffer(4)))(2,(CompactBuffer(b),CompactBuffer(5)))(3,(CompactBuffer(c),CompactBuffer(6)))(4,(CompactBuffer(),CompactBuffer(6)))
      */


  }
}
