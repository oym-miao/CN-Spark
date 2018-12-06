package sparkcore

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object LearnSparkKafka {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
//        conf.setMaster("local[3]")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("hdfs://bigdata01:9000/sparkdata5")
    val mapRDD=textFileRDD.map(line => line.toUpperCase())
    mapRDD.foreachPartition(partition =>{
      val props = new Properties()
      props.put("bootstrap.servers", "bigdata01:9092")
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String,String](props)
      partition.foreach(line =>{
        val message=new ProducerRecord[String, String]("cnwTopic",null,line)
        producer.send(message)
      })
      producer.close()
    })

  }
}
