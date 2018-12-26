package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/*通过reduceByKey来统计WordCount*/

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line=>line.split(" "))
    wordRdd.foreach(s=>print(s))


    //转换成元组，生成一个paiRdd
    val wordPairRdd=wordRdd.map(word=>(word,1))
    wordPairRdd.foreach(s=>print(s))



    val wordCounts = wordPairRdd.reduceByKey((x, y)=>x+y)

    //collect把worker上计算的结果收集到driver端
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)

  }

}
