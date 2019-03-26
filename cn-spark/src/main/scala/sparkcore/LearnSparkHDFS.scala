package sparkcore

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LearnSparkHDFS {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\HADOOP_HOME\\hadoop-2.5.0")
    val conf=new SparkConf()
    conf.setAppName("LearnSparkHDFS")
   //conf.setMaster("local[3]")

    //注意，这里不能这么设置，这是设置成standalone模式，提交的时候yarn 模式就无效了
  // conf.setMaster("spark://oym.com:7077")

    val sc =new SparkContext(conf)

    /**
      * 在向HDFS写入数据时，当前RDD的分区数，就是HDFS上的文件数。 之所以hdfs上面有很多小文件，是因为用多个分区写的
      * 为了避免HDFS上生成大量的小文件，可以对RDD进行reparation，然后再saveAsTextFile。
      * 集群运行时  这个路径应该是Linux上的路径。真实项目中，这个路径应该是HDFS上路径
      *
      * hdfs上面不建议存小文件，会增加namenode管理元数据的压力
      * 一般来说spark节点跟hdfs节点也就是计算节点跟存储节点会部署在同一台机器，这样就可以避免网络传输
      *
      */

    //将本地文件保存到本地指定的目录
    //textFileRDD.map(line => line.toUpperCase()).repartition(3).saveAsTextFile("file:///D:\\MyProject\\spark\\cn-spark\\out/hashPartition4")
    // val textFileRDD=sc.textFile("in/people.json")

    //StorageLevel



    //将Linux上的文件保存到hdfs上面,注意这个文件在每个节点都要有，并且路径一致
   // val textFileRDD=sc.textFile("/opt/modules/testdata/people.json") //这么写会报错！！ 说input path 不存在hdfs://ns:/opt/modules...,改成下面那个路径即可
    val textFileRDD=sc.textFile("file:///opt/modules/testdata/people.json")
    println("上传文件的count():===========>"+textFileRDD.count())
    textFileRDD.map(line => line.toUpperCase()).repartition(3).saveAsTextFile("hdfs://oym.com:8082/sparkdata5")




    /**
      *从HDFS上读取有5个分区数据，生成的RDD的分区数为5.
      * 注意：HDFS上一个小文件也是一个block。
      */
//    val textFileRDD5=sc.textFile("hdfs://bigdata01:9000/sparkdata5")
//    println("textFileRDD5 partition size:"+textFileRDD5.partitions.size)
  }
}
