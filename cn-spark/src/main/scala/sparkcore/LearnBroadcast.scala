package sparkcore

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object LearnBroadcast {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("LearnBroadcast")
    conf.setMaster("local[*]")
    val sc =new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //join
    var list1=List(("zhangsan",20),("lisi",22),("wangwu",26))
    var list2=List(("zhangsan","spark"),("lisi","kafka"),("zhaoliu","hive"))

    var rdd1=sc.parallelize(list1)
    var rdd2=sc.parallelize(list2)
    //这种方式存在性能问题，join引起shuffle。如何优化？
//    rdd1.join(rdd2).collect().foreach( t =>println(t._1+" : "+t._2._1+","+t._2._2))

    println("**************")
    //使用广播变量对join进行调优 使用场景：join两边的RDD，有一个RDD的数据量比较小，此时可以使用广播变量，将这个小的RDD广播出去，
    // 从而将普通的join，装换为map-side join。
//    val rdd1Data=rdd1.collectAsMap()
//    val rdd1BC=sc.broadcast(rdd1Data)
//    val rdd3=rdd2.mapPartitions(partition => {
//      val bc=rdd1BC.value
//      for{
//        (key,value) <-partition
//        if(rdd1Data.contains(key))
//      }yield(key,(bc.get(key).getOrElse(""),value))
//    })
//    rdd3.foreach(t => println(t._1+":"+t._2._1+","+t._2._2))

    var list3=List(("1","a"),("2","b"),("3","c"),("4","d"),("5","e"),("3","f"),("2","g"),("1","h"))
    var list4=List(("1","A"),("2","B"),("3","C"),("4","D"))

    val rddList3=sc.parallelize(list3,4)
    val rddList4=sc.parallelize(list4,4)
    println("rddList3 partition size:"+rddList3.partitions.size)
    println("rddList3 partitioner:"+rddList3.partitioner)
    println("rddList4 partition size:"+rddList4.partitions.size)
    println("rddList4 partitioner:"+rddList4.partitioner)
    val rdd5=rddList3.join(rddList4)
    println("rdd5.partitions.size:"+rdd5.partitions.size)
    println("rdd5.partitioner:"+rdd5.partitioner)
    rdd5.foreach(t => println(t._1+":"+t._2._1+","+t._2._2))

    val textFileRDD=sc.textFile("D:\\idea2018workspace\\cnw_spark_svn\\in\\goods")
    textFileRDD.filter(line =>line.contains("aa")).flatMap(line =>line.split(",")).map(word => ("aa",word)).reduceByKey((v1,v2)=>v1+","+v2)
      .union(textFileRDD.filter(line =>line.contains("bb")).flatMap(line =>line.split(",")).map(word => ("bb",word)).reduceByKey((v1,v2)=>v1+","+v2))
      .union(textFileRDD.filter(line =>line.contains("cc")).flatMap(line =>line.split(",")).map(word => ("cc",word)).reduceByKey((v1,v2)=>v1+","+v2))
 }
}
