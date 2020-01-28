package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnGroupByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnGroupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRdd: RDD[String] = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))

    //转换数据类型为pairRDD
    val pairRdd: RDD[(String, Int)] = dataRdd.map(x=>(x,1))

    val tuples: Array[(String, Iterable[Int])] = pairRdd.groupByKey().collect()

     tuples.foreach(print)

    /**
      * 输出结果
      * (two,CompactBuffer(1, 1))(one,CompactBuffer(1))(three,CompactBuffer(1, 1, 1))
       */


      //计算相同key的value相加的结果
      val totalCount: Array[(String, Int)] = tuples.map(x=>(x._1,x._2.sum))

      totalCount.foreach(x=>print(" 累计的结果: "+x))



  }


}
