package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class learnCountByValueJava {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("learnCountByValueJava").setMaster("local[*]");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<String> textFileRDD=jsc.textFile("in/README.md");



        JavaRDD<String> flatMapRDD=textFileRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //new class().var   （+）  tab键 .
        Map<String, Long> stringLongMap = flatMapRDD.countByValue();
        for (Map.Entry<String,Long> entry : stringLongMap.entrySet()){
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
    }
}
