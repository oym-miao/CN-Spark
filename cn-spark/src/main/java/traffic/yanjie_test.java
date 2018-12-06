package traffic;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import utils.MapUtil;
import utils.TestDataUtil;

import java.sql.Timestamp;
import java.util.Date;

/**
 * 套牌车分析算法实现
 * brave
 */
public class yanjie_test {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("TpcCompute").master("local").getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");

        Dataset<Row> cltgxxDF = spark.sql("select * from t_cltgxx t limit 10");
        cltgxxDF.show();

    }

}
