import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Владимир on 19.10.2016.
 */
public class SparkHelper {
    public static JavaPairRDD<Integer, List<String>> getPersons(JavaRDD<String> data, String field) {

        System.out.println(field.toUpperCase());
        JavaPairRDD<Integer, List<String>> pairRDD = data.flatMapToPair(s -> {
            ObjectMapper mapper = new ObjectMapper();
            List<Tuple2<Integer, List<String>>> list = new ArrayList<>();
            TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>(){};
            HashMap<String, Object> map = mapper.readValue(s.substring(s.indexOf("{")), type);
            String id = s.substring(s.indexOf("_") + 1, s.indexOf("{"));
            id = id.trim();
            List<HashMap<String, Object>> schools = (ArrayList<HashMap<String, Object>>)map.get(field);
            for (HashMap<String, Object> m: schools) {
                List<String> ids = new ArrayList<>();
                ids.add(id + "-" + m.get("name"));
                list.add(new Tuple2<>((Integer) m.get("id"), ids));
            }
            return list;
        }).persist(StorageLevel.MEMORY_AND_DISK());

        pairRDD = pairRDD.reduceByKey((s1, s2) -> {
            List<String> list = new ArrayList<>(s1);
            list.addAll(s2);
            return list;
        });

        return pairRDD;
    }

    public static <K, V> JavaPairRDD<K, V> sortedByValue(JavaPairRDD<K, V> data, Comparator<V> comparator) {
        JavaPairRDD<V, K> rdd = data.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        rdd = rdd.sortByKey(comparator);
        return rdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    }
}
