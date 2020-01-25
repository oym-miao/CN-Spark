package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object learnMapPartitions {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnMapPartitions")
    conf.setMaster("local[*]")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("in/README.md")

    //mapPartitions可以对一个RDD中所有的分区进行遍历
    //map每一个分区，然后再map分区中的每一个元素,这个partition是一个集合
    val mapPartitionRDD=textFileRDD.mapPartitions(partition => {
      partition.map(line =>line.toUpperCase)
    })

    //foreach是一个没有返回值的action
    mapPartitionRDD.foreach(line =>println(line))

    /**
      * 输出结果， 单词全部都转成大写了， 但是顺序有的是不对的，因为上面设置的是local[*]，多个线程并发读取
      *
      * # APACHE SPARK
      * TO RUN ONE OF THEM, USE `./BIN/RUN-EXAMPLE <CLASS> [PARAMS]`. FOR EXAMPLE:
      *
      *
      * SPARK IS A FAST AND GENERAL CLUSTER COMPUTING SYSTEM FOR BIG DATA. IT PROVIDES
      * ./BIN/RUN-EXAMPLE SPARKPI
      * HIGH-LEVEL APIS IN SCALA, JAVA, PYTHON, AND R, AND AN OPTIMIZED ENGINE THAT
      */




    val mapPartitionsWithIndexRDD=textFileRDD.mapPartitionsWithIndex((index,partition) => {
      partition.map(line =>index +" : "+line.toUpperCase)
    })

    //mapPartitionsWithIndexRDD.foreach(line =>println(line))

    val data = sc.parallelize(Array(('A',1),('b',2)))
    val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data.join(data2)
    //(A,(1,4)),(A,(1,6)),(b,(2,7))
    println(result.collect().mkString(","))
    /**
      * 输出：
      * A:(1,4)
      * A:(1,6)
      * b:(2,7)
      */
    result.foreach(t =>println(t._1+":("+t._2._1+","+t._2._2+")"))
  }
}
