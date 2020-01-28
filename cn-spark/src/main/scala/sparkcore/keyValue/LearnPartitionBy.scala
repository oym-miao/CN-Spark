package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
import sparkcore.partition.CustomPartition

object LearnPartitionBy {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnPartitionBy").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //val dataRDD: RDD[(String, Int)] = sc.parallelize(Array(("a",1),("b",2),("c",3),("d",4)),4)

    //HashPartitioner确定分区的方式：partition = key.hashCode () % numPartitions
    //dataRDD.partitionBy(new HashPartitioner(2))


    //传入自定义的分区器
    //运行后 可以看到key为a的 写入到了0分区，其他的都写到了1分区
    //val partitionRDD: RDD[(String, Int)] = dataRDD.partitionBy(new CustomPartition(2))
    //partitionRDD.saveAsTextFile("output")


    val rangeData: RDD[(Int, String)] = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(3,"d"),(4,"e")), 3)
    val rangeRDD: RDD[(Int, String)] = rangeData.partitionBy(new RangePartitioner(3,rangeData))
    rangeData.saveAsTextFile("output")



  }


}
