package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class learnCombineByKeyJava {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("learnCountByValueJava").setMaster("local[*]");
        JavaSparkContext jsc=new JavaSparkContext(conf);

        List<Tuple2<String, ScoreDetail>> tuple = Arrays.asList(
                new Tuple2<>("A", new ScoreDetail("A","Math",Float.valueOf(98))),
                new Tuple2<>("A", new ScoreDetail("A","English",Float.valueOf(88))),
                new Tuple2<>("B", new ScoreDetail("B","Math",Float.valueOf(75))),
                new Tuple2<>("B",new ScoreDetail("B","English",Float.valueOf(78))),
                new Tuple2<>("C",new ScoreDetail("C","Math",Float.valueOf(90))),
                new Tuple2<>("C",new ScoreDetail("C","English",Float.valueOf(80))),
                new Tuple2<>("D",new ScoreDetail("D","Math",Float.valueOf(91))),
                new Tuple2<>("D",new ScoreDetail("D","English",Float.valueOf(80)))
        );

        JavaPairRDD<String, ScoreDetail> stringScoreDetailJavaPairRDD = jsc.parallelizePairs(tuple);

        JavaPairRDD<String, ScoreDetail> reduceByKeyRDD = stringScoreDetailJavaPairRDD.reduceByKey(new Function2<ScoreDetail, ScoreDetail, ScoreDetail>() {
            @Override
            public ScoreDetail call(ScoreDetail v1, ScoreDetail v2) throws Exception {
                return new ScoreDetail(v1.getStudentName(),v1.getSubject()+","+v2.getSubject(),v1.getScore()+v2.getScore());
            }
        });
        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, ScoreDetail>>() {
            @Override
            public void call(Tuple2<String, ScoreDetail> t2) throws Exception {
                System.out.println(t2._1+":"+(t2._2.getScore()/(t2._2.getSubject().split(",").length)));
            }
        });


        Function<ScoreDetail, Tuple2<Float, Integer>> createCombiner = new Function<ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<>(scoreDetail.getScore(), 1);
            }
        };
        Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>> mergeValue = new Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp, ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<>(tp._1 + scoreDetail.getScore(), tp._2 + 1);
            }
        };

        Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> mergeCombiners = new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp1, Tuple2<Float, Integer> tp2) throws Exception {
                return new Tuple2<>(tp1._1 + tp2._1, tp1._2 + tp2._2);
            }
        };
        /**
         * *ByKey的函数,key是不会出现在计算过程中的，value会出现在计算过程中。如combineByKey函数实际上是combineValueByKey
         * 平均分的计算：总分数/科目数量
         *
         * createCombiner:combineByKey() 会遍历分区中的所有元素,对于每一个元素的key,只有两种情况：
         * 1、已经遍历过，这是第二次遍历到这个key---此时不会执行createCombiner函数，只会执行mergeValue函数
         * 2、从未遍历过，这是第一次遇到这个key---此时会执行createCombiner函数，创建这个key对应的累加器的初始值:本例中的new Tuple2<>(scoreDetail.getScore(), 1);
         * mergeValue：如果这是一个在处理当前分区之前已经遇到的key，会使用 mergeValue()方法将该key的累加器对应的当前值与这个新的值进行合并:
         * new Tuple2<>(tp._1 + scoreDetail.getScore(), tp1._2 + 1);
         * mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个key可以有多个累加器。如果有两个或者更
         * 多的分区都有对应同一个key的累加器， 就需要使用用户提供的 mergeCombiners() 将各个分区的结果进行合并。
         * new Tuple2<>(tp1._1 + tp2._1, tp1._2 + tp2._2);
         */
        JavaPairRDD<String, Tuple2<Float,Integer>> combineBykeyRDD =stringScoreDetailJavaPairRDD.combineByKey(createCombiner,mergeValue,mergeCombiners);

        //输出
        Map<String, Tuple2<Float, Integer>> stringTuple2Map = combineBykeyRDD.collectAsMap();
        for ( String key:stringTuple2Map.keySet()) {
            System.out.println(key+" "+stringTuple2Map.get(key)._1/stringTuple2Map.get(key)._2);
        }
    }
}
