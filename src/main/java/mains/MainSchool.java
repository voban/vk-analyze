package mains;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import services.SparkHelper;
import services.Task;

/**
 * Created by Владимир on 01.11.2016.
 */
public class MainSchool {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Владимир\\Desktop\\Java\\winutils");
        SparkConf conf = new SparkConf();
        conf.setAppName("VK-Analyze");
        if (conf.get("spark.master", null) == null) {
            conf.setMaster("local[*]");
        }
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> rddUsers = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\raw");
            JavaRDD<String> rddConnections = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\fast-graph");
            JavaRDD<String> csvSchools = SparkHelper.getAdditionalConnections(rddUsers, rddConnections, Task.SCHOOL);
            csvSchools.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\resSchools");

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
