

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import scala.Tuple2;
import scala.Tuple3;
import services.comparators.ComparatorListString;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Владимир on 18.10.2016.
 */
public class MainClass {

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
            JavaRDD<String> csvUniversities = SparkHelper.getAdditionalConnections(rddUsers, rddConnections, Task.UNIVERSITY);
            JavaRDD<String> csvMilitary = SparkHelper.getAdditionalConnections(rddUsers, rddConnections, Task.MILITARY);
            csvSchools.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\resSchools");
            csvUniversities.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\resUniversities");
            csvMilitary.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\resMilitary");

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
