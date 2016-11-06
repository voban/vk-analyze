package mains;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import services.SparkHelper;
import services.Task;

/**
 * Created by Владимир on 01.11.2016.
 */
public class MainMilitary {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Владимир\\Desktop\\Java\\winutils");
        SparkConf conf = new SparkConf();
        conf.setAppName("VK-Analyze");
        if (conf.get("spark.master", null) == null) {
            conf.setMaster("local[*]");
        }
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> rddUsers = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample2\\raw").coalesce(1);
            JavaRDD<String> rddConnections = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample2\\fast-graph").coalesce(1);
            JavaRDD<String> csvMilitary = SparkHelper.getAdditionalConnections(rddUsers, rddConnections, Task.MILITARY);
            csvMilitary.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\resMilitary");

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
