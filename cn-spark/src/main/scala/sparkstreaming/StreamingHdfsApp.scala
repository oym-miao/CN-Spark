package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时监听HDFS目录
  * checkpoint
  */
object StreamingHdfsApp {
  var input=""
  var output=""

  def func:StreamingContext={
    val sparkConf=new SparkConf().setAppName("StreamingHdfsApp")
    val ssc=new StreamingContext(sparkConf,Seconds(10))
    ssc.checkpoint("/user/work/checkpoint")
    val lineDStream:DStream[String]=ssc.textFileStream(input)
    val cainiaoLineDStream:DStream[String]=lineDStream.map(_+"cainiao5")
    val newCainiaoLineDStream:DStream[String]=cainiaoLineDStream.transform(x=>{
      x.flatMap(line=>line.split("="))
    })
    newCainiaoLineDStream.repartition(1).saveAsTextFiles(output)
    ssc
  }

  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      System.err.println("usage: Spark Streaming APP<input> <output>")
    }
    input=args(0)
    output=args(1)
    //检查点， checkpoint   用于容错，当程序崩溃时，你可以重启程序， 并让驱动程序driver从检查点恢复，这样SparkStreaming就可以读取
    //之前运行的程序处理数据的进度，并从那里继续

    val context:StreamingContext=StreamingContext.getOrCreate("/user/work/checkpoint",func _)
    context.start()
    context.awaitTermination()
    context.stop()

  }

}
