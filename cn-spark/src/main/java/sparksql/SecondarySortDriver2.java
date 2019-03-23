package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Spark 复杂的二次排序
 * 对应数据源：employee.csv
 * 数据字段说明：
 * First Name
 * Last Name
 * Job Titles
 * Department
 * Full or Part-Time
 * Salary
 * Typical Hours
 * Annual Salary
 * Hourly Rate
 * 根据Department字段对数据进行分组（如果这里不需要分组，那么就不需要分区器了），然后我们将首先根据salary排序，然后根据firstName排序。
 * 其实上面的这个需求用sparkSQL的窗口函数就能实现
 * @author Brave
 *
 */
public class SecondarySortDriver2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = jsc.textFile("in/employee.csv");
		
		JavaPairRDD<Employee_Key, Employee_Value> pair = rdd.mapToPair(new PairFunction<String, Employee_Key, Employee_Value>() {
				@Override
				public Tuple2<Employee_Key, Employee_Value> call(String data) throws Exception {
					String[] field = data.split(",");
					double salary = field[7].length() > 0 ? Double.parseDouble(field[7]) : 0.0;
					return new Tuple2<Employee_Key, Employee_Value>(
					new Employee_Key(field[0], field[3],salary),
					new Employee_Value(field[2], field[1]));
				}
		});
		
		//repartitionAndSortWithinPartitions 它传递我们的分区器，根据我们为key实现的Comparable接口，从而实现对数据进行排序,边分区边排序
		//如果分区数的数量比部门数少，那么就会出现多个部门被分布到同一个分区里面，如果是用hashcode的话
		//这里的分区是多少 可以计算出有多少个部门就设置多少个分区，是动态设置的，一个部门一个分区，边分区边排序
		JavaPairRDD<Employee_Key, Employee_Value> output = pair.repartitionAndSortWithinPartitions(new CustomEmployeePartitioner(50));
		for (Tuple2<Employee_Key, Employee_Value> data : output.collect()) {
			System.out.println(data._1.getDepartment() + " " + data._1.getSalary() + " " + data._1.getName()+ data._2.getJobTitle() + " " + data._2.getLastName());
		}
	}

}
