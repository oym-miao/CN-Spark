import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    println("Test!!!!!!!1")
    val conf=new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc=new SparkContext(config = conf)
    val count=sc.textFile("D:\\idea2018workspace\\cnw_spark_svn\\pom.xml").count()
    println(count)
  }
}
