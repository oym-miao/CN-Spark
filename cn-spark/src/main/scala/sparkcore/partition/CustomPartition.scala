package sparkcore.partition

import org.apache.spark.Partitioner

class CustomPartition(numParts: Int) extends Partitioner{

  // 这个方法需要返回你想要创建分区的个数
  override def numPartitions: Int = numParts


  // 这个函数需要对输入的key做计算，
  // 然后返回该key的分区ID，范围一定是0到numPartitions-1
  override def getPartition(key: Any): Int = {

    val inputKey: String = key.toString

    val hashCode: Int = inputKey.hashCode%numPartitions

    if("a".equals(inputKey)){
        0
      }else{
        //hashCode
        1
      }
  }


  // Java标准的判断相等的函数，
  // 之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样
  override def equals(other: Any): Boolean = other match {
    case test:CustomPartition  =>test.numPartitions == numPartitions
    case _ =>
      false
  }
}
