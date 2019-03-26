package sparkcore

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object LearnBroadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("LearnBroadcast")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //变量i是是dirver端的，而计算是在executor里面，这样是不会改变driver端的变量的，所以打印输出还是0!!
    //可以通过共享变量的方式解决该问题
    val textFileRDD = sc.textFile("in/README.md")
 /*   val count = textFileRDD.count()
    var i = 0
    val mapRDD = textFileRDD.map(line => {
      i = i + 1
      (line, line.size)
    })

    println("count:" + count)
    println("i:" + i)*/




      //val count=textFileRDD.count()
        //项目里面不到万不得已不要用累加器
        //记住了，累加器只能在driver端获取值，在task里面添加1 !!!
        val acc=sc.longAccumulator("counterAcc")
        val mapRDD=textFileRDD.map(line => {
          //这段代码是在task里面执行的，所以task只能对它进行累加
          acc.add(1)
          //println(acc.value)
          (line, line.size)
        })
        //运行多个action累加器是会成倍往上涨的！！！ 如果要想执行两个action,又不让累加器成倍往上涨，可以通过缓存
        //只所以累加器执行一个action就会往上涨一倍，是因为每执行一个action,都会导致action之前的各种transformation操作 从头到尾执行一遍！！！！
        //第一次真正缓存的地方是在第一次action执行的地方，缓存不是一个action，它不会立马执行
        mapRDD.cache()
        val count=mapRDD.count()
        println("count:"+count)
        println("acc.value:"+acc.value)
        println("********************")
        //如果上面执行action操作之前没有缓存，那么下面的累加器输出还是会翻倍
        //mapRDD.cache()
        val count2=mapRDD.count()
        println("count:"+count2)
        println("acc.value:"+acc.value)


    //join
    /*  var list1=List(("zhangsan",20),("lisi",22),("wangwu",26))
      var list2=List(("zhangsan","spark"),("lisi","kafka"),("zhaoliu","hive"))

      var rdd1=sc.parallelize(list1)
      var rdd2=sc.parallelize(list2)
      //这种方式存在性能问题，join引起shuffle。如何优化？
      rdd1.join(rdd2).collect().foreach( t =>println(t.toString()+"=====> "+ t._1+" : "+t._2._1+","+t._2._2))*/

    /*
      数据集结果
      (zhangsan,(20,spark))=====> zhangsan : 20,spark
      (lisi,(22,kafka))=====> lisi : 22,kafka

      */

    println("**************")
    //通过广播变量来进行join，达到优化的效果，数据量大才能明显看到效果
    //使用广播变量对join进行调优 使用场景：join两边的RDD，有一个RDD的数据量比较小，此时可以使用广播变量，将这个小的RDD广播出去，
    //这个大还是小是相对于资源的！！！
    //发生shuffle的原因是:所需要的数据存在于不同的节点，要把它拉到同一个节点才能计算，这个时候就会发生shuffle!!!!
    //rdd是不能被广播出去的，因为rdd里面是不含有数据的，只有执行action操作里面才有数据的
    // 从而将普通的join，装换为map-side join。
    //通过collectAsMap把数据给搜集过来，这个时候才能对它进行广播
    /*    val rdd1Data=rdd1.collectAsMap()
       // 广播的时候会返回一个变量，用这个变量跟rdd进行join就行了
        val rdd1BC=sc.broadcast(rdd1Data)
        val rdd3=rdd2.mapPartitions(partition => {
            //在这个partition里面 先获取这个broadcast
          val bc=rdd1BC.value
          for{
              //使用partition里面的key value
            (key,value) <-partition
            if(rdd1Data.contains(key))
           // yield是把上面符合条件的数据收集起来进行返回，因为mapPartitions是需要返回集的
          }yield(key,(bc.get(key).getOrElse(""),value))
        })
        rdd3.foreach(t => println(t._1+":"+t._2._1+","+t._2._2))*/


    //    var list3=List(("1","a"),("2","b"),("3","c"),("4","d"),("5","e"),("3","f"),("2","g"),("1","h"))
    //    var list4=List(("1","A"),("2","B"),("3","C"),("4","D"))
    //
    //    val rddList3=sc.parallelize(list3,4)
    //    val rddList4=sc.parallelize(list4,4)
    //    println("rddList3 partition size:"+rddList3.partitions.size)
    //    println("rddList3 partitioner:"+rddList3.partitioner)
    //    println("rddList4 partition size:"+rddList4.partitions.size)
    //    println("rddList4 partitioner:"+rddList4.partitioner)
    //    val rdd5=rddList3.join(rddList4)
    //    println("rdd5.partitions.size:"+rdd5.partitions.size)
    //    println("rdd5.partitioner:"+rdd5.partitioner)
    //    rdd5.foreach(t => println(t._1+":"+t._2._1+","+t._2._2))
    //



    //    val textFileRDD=sc.textFile("D:\\idea2018workspace\\cnw_spark_svn\\in\\goods")
    //    textFileRDD.filter(line =>line.contains("aa")).flatMap(line =>line.split(",")).map(word => ("aa",word)).reduceByKey((v1,v2)=>v1+","+v2)
    //      .union(textFileRDD.filter(line =>line.contains("bb")).flatMap(line =>line.split(",")).map(word => ("bb",word)).reduceByKey((v1,v2)=>v1+","+v2))
    //      .union(textFileRDD.filter(line =>line.contains("cc")).flatMap(line =>line.split(",")).map(word => ("cc",word)).reduceByKey((v1,v2)=>v1+","+v2))
  }


}
