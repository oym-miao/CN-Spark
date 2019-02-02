package sparkstreaming

 
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 热点搜索词滑动统计
  * 每隔10s，统计最近60s的搜索词的搜索频次，并打印出排名最靠前的3个搜索词及其上出现的频次
  */
object StreamingWindowApp {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      System.err.println("usage: Spark Streaming APP<host> <port>")
    }
    val host=args(0)
    val port=args(1).toInt

    val sparkConf=new SparkConf().setAppName("StreamingWindowApp")
    val ssc:StreamingContext=new StreamingContext(sparkConf,Seconds(5))
    //通过streaming接受socket数据 val stream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.200.100",9999)
    val searchDStream:DStream[String]=ssc.socketTextStream(host,port)

    //切分每一行,每个单词记为1
    val searchWordPairDStream:DStream[(String,Int)]=searchDStream.flatMap(_.split(" ")).map((_,1))

    //reduceFunc：需要一个函数
    //windowDuration 窗口长度是60s
    //slideDuration 滑动间隔是10s 即每隔多久计算一次
    val searchWordCountDStream:DStream[(String,Int)]=searchWordPairDStream.reduceByKeyAndWindow(
      (x:Int,y:Int)=>x+y,Seconds(60),Seconds(10))


    searchWordCountDStream.foreachRDD(x=>{
      x.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(3).foreach(x=>{
        println("------------------top 3 word is :"+x)
      })
    })
    ssc.start() //启动执行计划
    ssc.awaitTermination()  //等待程序停止,执行期间发出的异常都将会抛出
    ssc.stop()

  }

}
