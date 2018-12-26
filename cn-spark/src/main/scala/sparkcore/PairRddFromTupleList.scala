package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //1.创建 一个RDD
  /*  val tuple = List(("zhangsan","spark"),("oym","spark"), ("lisi","kafka"), ("Mary","hadoop"), ("James","hive"))
    val pairRDD = sc.parallelize(tuple) //生成paiRDD
    pairRDD.foreach(s=>println(s))
    //取出tuple中value等于spark的元素
    val filterRDD=pairRDD.filter(t =>t._2.equals("spark"))
    //访问tuple里面的元素是通过 _加下标进行访问 并且下标是从1开始的
    filterRDD.foreach(t =>println(t._1+","+t._2))*/


    //2.将一个常规的RDD转换为Pair RDD
    val inputStrings = List("Lily 165cm 23", "Jack 170cm 29", "Mary 175cm 29", "James 180cm 8")
    val regularRDDs = sc.parallelize(inputStrings)
    //tuple中的key 我取它的第0个元素，value取它的第2个元素，这样一组合就形成了一个pairRDD
    val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(2)))
    pairRDD.foreach(s=>print(s))



  }
}
