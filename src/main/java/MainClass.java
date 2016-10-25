

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import scala.Tuple2;
import services.comparators.ComparatorListString;

import java.util.*;

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
            JavaRDD<String> rddAll = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\raw");

            JavaRDD<String> rddSchools = rddAll.filter(s -> {
                ObjectMapper mapper = new ObjectMapper();
                TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>(){};
                HashMap<String, Object> map = mapper.readValue(s.substring(s.indexOf("{")), type);
                return map.containsKey("schools");
            });

            JavaPairRDD<Integer, List<String>> pairRDD = SparkHelper.getPersons(rddSchools, "schools");

            Comparator comparator = new ComparatorListString();
            JavaPairRDD<Integer, List<String>> result = SparkHelper.sortedByValue(pairRDD, comparator);
            result.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\Java\\vk-sample\\result\\result1");

            for (Tuple2<Integer, List<String>> t: result.take(20)) {
                System.out.print("ID школы: " + t._1 + " | ");
                System.out.print(t._2.size() + " человек:");
                for (String s : t._2)
                    System.out.print("     " + s);
                System.out.println();
                System.out.println();
            }
            System.out.println();

            JavaRDD<String> rddUniversities = rddAll.filter(s -> {
                ObjectMapper mapper = new ObjectMapper();
                TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>(){};
                HashMap<String, Object> map = mapper.readValue(s.substring(s.indexOf("{")), type);
                return map.containsKey("universities");
            });

            pairRDD = SparkHelper.getPersons(rddUniversities, "universities");
            result = SparkHelper.sortedByValue(pairRDD, comparator);
            result.coalesce(1).saveAsTextFile("C:\\Users\\Владимир\\Desktop\\Java\\vk-sample\\result\\result2");

            for (Tuple2<Integer, List<String>> t: result.take(20)) {
                System.out.print("ID университета: " + t._1 + " | ");
                System.out.print(t._2.size() + " человек:");
                for (String s : t._2)
                    System.out.print("     " + s);
                System.out.println();
                System.out.println();
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
