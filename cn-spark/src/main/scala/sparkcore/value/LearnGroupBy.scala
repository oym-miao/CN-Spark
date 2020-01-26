package sparkcore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnGroupBy {

  def main(args: Array[String]): Unit = {
    
    val conf: SparkConf = new SparkConf().setAppName("LearnGroupBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //需求: 创建一个RDD,按照元素模以2的值进行分组
    //分组后的数据形成了对偶元组 (K-V)，K表示分组的key,V表示分组的集合
    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)

    groupRDD.collect().foreach(println)

    /**
      * 输出结果
      *
      * (0,CompactBuffer(2, 4))
      * (1,CompactBuffer(1, 3))
      */

    
  }
}
