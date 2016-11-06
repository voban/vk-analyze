package mains;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;
import services.SparkHelper;
import services.Task;

import java.io.FileWriter;
import java.util.*;

/**
 * Created by Владимир on 01.11.2016.
 */
public class MainTester {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:\\Users\\Владимир\\Desktop\\Java\\winutils");
        SparkConf conf = new SparkConf();
        conf.setAppName("VK-Analyze");
        if (conf.get("spark.master", null) == null) {
            conf.setMaster("local[*]");
        }
        try (JavaSparkContext sc = new JavaSparkContext(conf);
             FileWriter writer = new FileWriter("C:\\Users\\Владимир\\Desktop\\vk-sample\\career75.txt");) {

            /*JavaRDD<String> placesBefore = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\results\\careerBefore\\part-00000");
            List<String> matched = new ArrayList<>();
            List<String> notMatched = new ArrayList<>();
            List<Integer> numbers = new ArrayList<>();
            List<String> places = placesBefore.collect();
            for (int i = 0; i < places.size() - 1; i++) {
                String place1 = places.get(i);
                for (int j = i + 1; j < places.size(); j++) {
                    String place2 = places.get(j);
                    String place1Normal = place1
                            .toLowerCase()
                            .replaceAll("ё", "е")
                            .replaceAll("[^a-zа-я0-9]", "  ")
                            .replaceAll("( [а-я] |^[а-я] | [а-я]$)", " ")
                            .replaceAll(" {2,}", " ")
                            .trim();
                    String place2Normal = place2
                            .toLowerCase()
                            .replaceAll("ё", "е")
                            .replaceAll("[^a-zа-я0-9]", "  ")
                            .replaceAll("( [а-я] |^[а-я] | [а-я]$)", " ")
                            .replaceAll(" {2,}", " ")
                            .trim();
                    if ((place1Normal.length() > 0) && (place2Normal.length() > 0)) {
                        Set<String> set1 = new HashSet<>(Arrays.asList(place1Normal.split(" ")));
                        Set<String> set2 = new HashSet<>(Arrays.asList(place2Normal.split(" ")));
                        int max = Math.max(set1.size(), set2.size());
                        int min = Math.min(set1.size(), set2.size());
                        set1.retainAll(set2);
                        if (((double) set1.size() / max >= 0.75) && (set1.size() >= min)) {
                            matched.add(place1 + "     |||    " + place2);
                            if (!numbers.contains(i))
                                numbers.add(i);
                            if (!numbers.contains(j))
                                numbers.add(j);
                        }
                    }
                }
                if (!numbers.contains(i))
                    notMatched.add(place1);
            }
            writer.write("ПАРЫ СОВПАВШИХ НАЗВАНИЙ:" + "\n\n");
            for (String s : matched)
                writer.write(s + "\n");
            writer.write("\n\n\n" + "НЕСОВПАВШИЕ НАЗВАНИЯ:" + "\n\n");
            for (String s : notMatched)
                writer.write(s + "\n");*/


            /*JavaRDD<String> rddUsers = sc.textFile("C:\\Users\\Владимир\\Desktop\\vk-sample\\raw");
             JavaRDD<String> placesBefore= rddUsers.flatMap(s -> {
                List<String> list = new ArrayList<>();
                ObjectMapper mapper = new ObjectMapper();
                String[] st = s.split("\\t");
                Map user = mapper.readValue(st[1].trim(), Map.class);
                if ("work".equals(user.get("occupation_type"))) {
                    String work = (String) user.get("occupation_name");
                    list.add(work);
                }
                if (user.get("career") != null) {
                    List<Map<String, Object>> career = (List<Map<String, Object>>) user.get("career");
                    for (Map<String, Object> map : career)
                        if (map.get("company") != null)
                            list.add((String) map.get("company"));
                }
                return list;
            });*/


        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
