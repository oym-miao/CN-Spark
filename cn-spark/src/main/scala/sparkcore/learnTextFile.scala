package sparkcore

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object learnTextFile {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()

    /**
      *
      * local:一个线程
      * local[*]:服务器core数量的相差
      * local[4]:4个线程
      *
      */


    conf.setAppName("learnTextFile").setMaster("local")
//    conf.setMaster("local[*]")
//    conf.set("spark.executor.memory","500m")
//    conf.set("spark.executor.cores","1")
    val sc =new SparkContext(conf)
//    sc.setLogLevel("ERROR")


  //val textFileRDD=sc.textFile("in/README.md")

    //这个length就等于你的核心数，它启动了几个partion!
    //textFileRDD.partitions.length

    //以每一行作为参数，每一行的值进行返回，map==> 一对一
  /*  val uppercaseRDD = textFileRDD.map(line=>line.split(" "))
    for (elem <- uppercaseRDD.take(3)) {
      println(elem)
    }*/


//    val uppercaseRDD = textFileRDD.flatMap(line=>line.split(" "))
//    for (elem <- uppercaseRDD.take(3)) {
//      println(elem)
//    }


//    println(mapRDD.getStorageLevel.description)
//    println(mapRDD.getStorageLevel)
//    mapRDD.cache()
//    mapRDD.map(t =>(t._2,t._1)).top(6).foreach(t =>println(t._1+" : "+t._2))
//   println( mapRDD.toDebugString)
//    val count=textFileRDD.count()
//    println("count:"+count)
//    val uppercaseRDD=textFileRDD.map(line => line)
//    for (elem <- uppercaseRDD.take(3)) {
//      println(elem)
//    }
//    textFileRDD.map(line => line.toUpperCase()).saveAsTextFile("hdfs://bigdata01:9000/sparkdata2")
//    val flatMapRDD=textFileRDD.flatMap(line =>line.split(" "))
//    flatMapRDD.take(3).foreach(word => println(word))
//    println("count:"+count)
//


      //union并集
   /* val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
    val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
    val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))

    val rddUnion = rdd1.union(rdd2).union(rdd3)
    rddUnion.foreach(println)*/


    //intersection 交集
//    val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014), (16,"feb",2014)))
//    val rdd2 = sc.parallelize(Seq((5,"dec",2014),(1,"jan",2016),(16,"feb",2015)))
//    val comman = rdd1.intersection(rdd2)
//    comman.foreach(println)




    sc.stop()
  }
}
