package sparkcore.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnCombineByKey {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("LearnCombineByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.parallelize(List(("张三",10),("张三",20),("李四",30),("李四",10),("王五",30),("张三",10)),2)

    // 第一个参数的转换 ("张三",10)=>("张三",(10,1)) 即把源tuple的v转换成value固定为1的元组
    // 第二个参数,分区内的计算  (acc:(Int,Int),v)=>(acc._1+v,acc._2+1)=((10,1),20)=>(10+20,1+1)
    //第三个参数，分区间的计算  (acc1:(Int,Int),acc2:(Int,Int))=((30,2),(10,1))=>(30+10,2+1)
    val combinBykeyRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc2._2+acc2._2))

    val tuples: Array[(String, (Int, Int))] = combinBykeyRDD.collect()

    tuples.foreach(print)

    /**
      * 输出结果
      * (张三,(40,2))(李四,(40,2))(王五,(30,1))
      */

    //统计出每个相同key的次数后，再进行平均值的计算
    val averageRDD: RDD[(String, Int)] = combinBykeyRDD.map(x=>(x._1,x._2._1/x._2._2))
    averageRDD.collect().foreach(print)

    /**
      * 输出结果
      * (张三,20)(李四,20)(王五,30)
      */


  }

}
