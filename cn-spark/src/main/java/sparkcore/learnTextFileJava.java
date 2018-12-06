package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class learnTextFileJava {
    public static  void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("learnTextFileJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFileRDD=sc.textFile("in/README.md");
     /*   textFileRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return null;
            }
        });*/
        Long count=textFileRDD.count();
        System.out.println("count:"+count);

    }
}
