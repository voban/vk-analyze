package services;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;
import services.comparators.ComparatorListString;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Владимир on 19.10.2016.
 */
public class SparkHelper {

    public static JavaRDD<String> getAdditionalConnections(JavaRDD<String> users, JavaRDD<String> connections, Task task) throws Exception {

        /*
        * Фильтрую людей по признаку наличия информации в профиле,
        * затем выделяю из всей указанной информации лишь необходимую,
        * например, о школах.
         */
        if (task.getName().equals("career"))
            return getAdditionalConnectionsCareer(users, connections);
        JavaPairRDD<String, Map<String, Object>> rddUsers = users
                .flatMapToPair(s -> {
                    ObjectMapper mapper = new ObjectMapper();
                    String[] st = s.split("\\t");
                    Map user = mapper.readValue(st[1].trim(), Map.class);
                    List<Tuple2<String, Map<String, Object>>> list = new ArrayList<>();
                    if (user.get(task.getName()) != null) {
                        String vkId = st[0].trim();
                        List<Map<String, Object>> data = (List<Map<String, Object>>) user.get(task.getName());
                        int bdate = 0;
                        if (user.get("bdate") != null) {
                            SimpleDateFormat format = new SimpleDateFormat("d.M.yyyy");
                            try {
                                Date date = format.parse((String) user.get("bdate"));
                                GregorianCalendar cal = new GregorianCalendar();
                                cal.setTime(date);
                                bdate = cal.get(GregorianCalendar.YEAR);
                            } catch (Exception e) {
                            }
                        }
                        for (Map<String, Object> map : data) {
                            map.put("bdate", bdate);
                            list.add(new Tuple2<>(vkId, map));
                        }
                    }
                    return list;
                });

        /*
        * Оставляю только уникальные связи типа "Друг".
         */
        JavaRDD<String> rddConnections = connections.distinct();

        JavaPairRDD<String, String> rddFriendsConnections = rddConnections
                .flatMapToPair(s -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    String[] line = s.split("\t");
                    if ((line[0].compareTo(line[1]) < 0) && (line[2].equals("0")))
                        list.add(new Tuple2<>(line[0], line[1]));
                    return list;
                });

        /*
        * Создаю единую базу данных.
        * Образец строки: id_vk1 | id_vk2 | школа, в которой учился id_vk1 | школа, в которой учился id_vk2
         */
        JavaRDD<Tuple4<String, String, Map<String, Object>, Map<String, Object>>> database = rddFriendsConnections.join(rddUsers)
                .mapToPair(t -> new Tuple2<>(t._2()._1(), new Tuple2<>(t._1(), t._2()._2())))
                .join(rddUsers)
                .map(t -> new Tuple4<>(t._2._1._1, t._1, t._2._1._2, t._2._2));

        /*
        * Из базы данных выделяю новые связи.
         */
        JavaRDD<String> csv = database.flatMap(t -> {
            Map mapFrom = t._3();
            Map mapTo = t._4();
            List<String> list = new ArrayList<>();
            switch (task) {
                case SCHOOL:
                    equalsSchools(mapFrom, mapTo, list, t);
                    break;
                case UNIVERSITY:
                    equalsUniversities(mapFrom, mapTo, list, t);
                    break;
                case MILITARY:
                    equalsMilitary(mapFrom, mapTo, list, t);
                    break;
            }
            return list;
        });

        /*
        * Далее проводится анализ найденных связей по типам,
        * возвращаются только те, которых не было в fastgraph.
         */
        JavaPairRDD<String, Integer> pairRDD = csv.
                map(t -> {
                    String[] line = t.split("\t");
                    return new Tuple3<>(line[0], line[1], line[2]);
                })
                .mapToPair(t -> new Tuple2<>(t._3(), 1))
                .reduceByKey((a, b) -> a + b);

        FileWriter writer = new FileWriter("C:\\Users\\Владимир\\Desktop\\vk-sample\\" + task.getName() + ".txt");
        writer.write("Найдено новых связей - " + csv.count() + "\n");
        for (Tuple2<String, Integer> t : pairRDD.collect()) {
            double d = (double) t._2 / csv.count() * 100;
            writer.write(String.format("Связь " + t._1 + " - " + t._2 + " человек, " + "%.1f" + " процентов", d));
            writer.append("\n");
        }
        JavaRDD<String> unique = csv.subtract(rddConnections);
        writer.write("Найдено уникальных связей - " + unique.count() + "\n");
        writer.close();

        return unique;
    }

    public static JavaRDD<String> getAdditionalConnectionsCareer(JavaRDD<String> users, JavaRDD<String> connections) throws Exception {

        FileWriter f = new FileWriter("C:\\Users\\Владимир\\Desktop\\vk-sample\\resCareer.txt");
        JavaPairRDD<String, Tuple2<Integer, String>> rddUsers = users
                .flatMapToPair(s -> {
                    ObjectMapper mapper = new ObjectMapper();
                    String[] st = s.split("\\t");
                    Map user = mapper.readValue(st[1].trim(), Map.class);
                    List<Tuple2<String, Tuple2<Integer, String>>> list = new ArrayList<>();
                    if ((user.get("career") != null) || ("work".equals(user.get("occupation_type")))) {
                        String vkId = st[0].trim();
                        Integer occupationId = 0;
                        if (("work".equals(user.get("occupation_type"))) && (user.get("occupation_id") != null))
                            occupationId = (Integer) user.get("occupation_id");
                        List<String> places = new ArrayList<>();
                        if (user.get("career") != null) {
                            List<Map<String, Object>> data = (List<Map<String, Object>>) user.get("career");
                            for (Map<String, Object> map : data)
                                if (map.get("company") != null)
                                    places.add((String) map.get("company"));
                        }
                        if ("work".equals(user.get("occupation_type")))
                            places.add((String) user.get("occupation_name"));
                        for (String place : places)
                            list.add(new Tuple2<>(vkId, new Tuple2<>(occupationId, place)));
                    }
                    return list;
                });

        f.write(String.valueOf(rddUsers.count()) + "\n");
        JavaRDD<String> rddConnections = connections.distinct();

        JavaPairRDD<String, String> rddFriendsConnections = rddConnections
                .flatMapToPair(s -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    String[] line = s.split("\t");
                    if ((line[0].compareTo(line[1]) < 0) && (line[2].equals("0")))
                        list.add(new Tuple2<>(line[0], line[1]));
                    return list;
                });
        f.write(String.valueOf(rddFriendsConnections.count()) + "\n");

        JavaPairRDD<String, Tuple2<String, Tuple2<Integer, String>>> join1 = rddFriendsConnections.join(rddUsers);
        f.write(String.valueOf(join1.count()) + "\n");

        JavaPairRDD<String, Tuple3<String, Integer, String>> reverse = join1.mapToPair(t ->
                new Tuple2<>(t._2()._1(), new Tuple3<>(t._1(), t._2()._2()._1(), t._2()._2()._2())));
        f.write(String.valueOf(reverse.count()) + "\n");

        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, String>, Tuple2<Integer, String>>> join2 = reverse.join(rddUsers);
        f.write(String.valueOf(join2.count()) + "\n");

        JavaRDD<Tuple6<String, String, Integer, String, Integer, String>> database = join2.map(t ->
                new Tuple6<>(t._2()._1()._1(), t._1(), t._2()._1()._2(), t._2()._1()._3(), t._2()._2()._1(), t._2()._2()._2()));

        f.write(String.valueOf(database.count() + "\n"));
        JavaRDD<String> csv = database.flatMap(t -> {
            List<String> list = new ArrayList<>();
            Integer occupationId1 = t._3();
            Integer occupationId2 = t._5();
            String place1 = t._4();
            String place2 = t._6();
            if (((occupationId1 != 0) && (occupationId2 != 0) && occupationId1.equals(occupationId2))
            || (equalsWorkplaces(place1, place2))){
                list.add(t._1() + "\t" + t._2() + "\t" + "27");
                list.add(t._2() + "\t" + t._1() + "\t" + "27");
            }
            return list;
        });
        f.close();

        JavaRDD<String> unique = csv.distinct();
        return unique;
    }

    /*
    * Сравнение школ:
    * Если равны года рождения, либо год начала или конца обучения, то сильная связь; иначе - слабая
     */
    public static void equalsSchools(Map mapFrom, Map mapTo, List list, Tuple4 t) {
        if ((mapFrom.get("id") == null) || (mapTo.get("id") == null))
            return;
        if ((mapFrom.get("id")).equals(mapTo.get("id"))) {
            Integer bdateFrom = (Integer) mapFrom.get("bdate");
            Integer bdateTo = (Integer) mapTo.get("bdate");
            Integer yearStartFrom = mapFrom.get("year_from") != null ? (Integer) mapFrom.get("year_from") : 0;
            Integer yearStartTo = mapTo.get("year_from") != null ? (Integer) mapTo.get("year_from") : 0;
            Integer yearEndFrom = mapFrom.get("year_to") != null ? (Integer) mapFrom.get("year_to") : 0;
            Integer yearEndTo = mapTo.get("year_to") != null ? (Integer) mapTo.get("year_to") : 0;
            if (((bdateFrom != 0) && (bdateTo != 0) && (Math.abs(bdateFrom - bdateTo) < 2))
                    || ((yearStartFrom != 0) && (yearStartTo != 0) && (yearStartFrom.equals(yearStartTo)))
                    || ((yearEndFrom != 0) && (yearEndTo != 0) && (yearEndFrom.equals(yearEndTo)))) {
                list.add(t._1() + "\t" + t._2() + "\t" + "37");
                list.add(t._2() + "\t" + t._1() + "\t" + "37");
            } else {
                list.add(t._1() + "\t" + t._2() + "\t" + "26");
                list.add(t._2() + "\t" + t._1() + "\t" + "26");
            }
        }
    }

    /*
    * Сравнение ВУЗов:
    * Если полное совпадение годов рождения или года выпуска + одинаковый факультет, то сильная связь.
    * Если примерно совпадает год рождения или год выпуска, то слабая связь.
     */
    public static void equalsUniversities(Map mapFrom, Map mapTo, List list, Tuple4 t) {
        if ((mapFrom.get("id") == null) || (mapTo.get("id") == null))
            return;
        if ((mapFrom.get("id")).equals(mapTo.get("id"))) {
            Integer bdateFrom = (Integer) mapFrom.get("bdate");
            Integer bdateTo = (Integer) mapTo.get("bdate");
            Integer yearGraduateFrom = mapFrom.get("graduation") != null ? (Integer) mapFrom.get("graduation") : 0;
            Integer yearGraduateTo = mapTo.get("graduation") != null ? (Integer) mapTo.get("graduation") : 0;
            if (((bdateFrom != 0) && (bdateTo != 0) && (Math.abs(bdateFrom - bdateTo) < 2))
                    || ((yearGraduateFrom != 0) && (yearGraduateTo != 0) && (Math.abs(yearGraduateFrom - yearGraduateTo) < 2)))
                if ((((bdateFrom != 0) && (bdateFrom.equals(bdateTo)))
                        || ((yearGraduateFrom != 0) && (yearGraduateFrom.equals(yearGraduateTo))))
                        && (mapFrom.get("faculty").equals(mapTo.get("faculty")))) {
                    list.add(t._1() + "\t" + t._2() + "\t" + "38");
                    list.add(t._2() + "\t" + t._1() + "\t" + "38");
                } else {
                    list.add(t._1() + "\t" + t._2() + "\t" + "25");
                    list.add(t._2() + "\t" + t._1() + "\t" + "25");
                }
        }
    }

    /*
    * Сравнение воинских частей:
    * Либо одинаковый год рождения, либо год начала или конца службы
     */
    public static void equalsMilitary(Map mapFrom, Map mapTo, List list, Tuple4 t) {
        if ((mapFrom.get("unit_id") == null) || (mapTo.get("unit_id") == null))
            return;
        if ((mapFrom.get("unit_id")).equals(mapTo.get("unit_id"))) {
            Integer bdateFrom = (Integer) mapFrom.get("bdate");
            Integer bdateTo = (Integer) mapTo.get("bdate");
            Integer yearStartFrom = mapFrom.get("from") != null ? (Integer) mapFrom.get("from") : 0;
            Integer yearStartTo = mapTo.get("from") != null ? (Integer) mapTo.get("from") : 0;
            Integer yearEndFrom = mapFrom.get("until") != null ? (Integer) mapFrom.get("until") : 0;
            Integer yearEndTo = mapTo.get("until") != null ? (Integer) mapTo.get("until") : 0;
            if (((bdateFrom != 0) && (bdateTo != 0) && (Math.abs(bdateFrom - bdateTo) < 2))
                    || ((yearStartFrom != 0) && (yearStartTo != 0) && (yearStartFrom.equals(yearStartTo)))
                    || ((yearEndFrom != 0) && (yearEndTo != 0) && (yearEndFrom.equals(yearEndTo)))) {
                list.add(t._1() + "\t" + t._2() + "\t" + "36");
                list.add(t._2() + "\t" + t._1() + "\t" + "36");
            }
        }
    }

    /*
    * Сравнение двух названий мест работы
     */
    public static boolean equalsWorkplaces(String place1, String place2) {
        String place1Normal = getNormalWorkplace(place1);
        String place2Normal = getNormalWorkplace(place2);
        if ((place1Normal.length() == 0) || (place2Normal.length() == 0))
            return false;
        Set<String> set1 = new HashSet<>(Arrays.asList(place1Normal.split(" ")));
        Set<String> set2 = new HashSet<>(Arrays.asList(place2Normal.split(" ")));
        int max = Math.max(set1.size(), set2.size());
        int min = Math.min(set1.size(), set2.size());
        set1.retainAll(set2);
        if (((double) set1.size() / max >= 0.75) && (set1.size() >= min))
            return true;
        else return false;
    }

    /*
    * Получение нормализованного названия места работы
     */
    public static String getNormalWorkplace(String place) {
        String placeNormal = place
                .toLowerCase()
                .replaceAll("ё", "е")
                .replaceAll("[^a-zа-я0-9]", "  ")
                .replaceAll("( [а-я] |^[а-я] | [а-я]$|^[а-я]$)", " ")
                .replaceAll(" {2,}", " ")
                .trim();
        return placeNormal;
    }

    public static JavaPairRDD<Integer, List<String>> getSortedInformation(JavaRDD<String> data, String field) {

        JavaPairRDD<Integer, List<String>> pairRDD = data
                .filter(s -> {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> map = mapper.readValue(s.substring(s.indexOf("{")), Map.class);
                    return map.containsKey(field);
                })
                .flatMapToPair(s -> {
                    ObjectMapper mapper = new ObjectMapper();
                    List<Tuple2<Integer, List<String>>> list = new ArrayList<>();
                    Map<String, Object> map = mapper.readValue(s.substring(s.indexOf("{")), Map.class);
                    String id = (String) map.get("id_vk");
                    List<HashMap<String, Object>> info = (ArrayList<HashMap<String, Object>>) map.get(field);
                    for (HashMap<String, Object> m : info) {
                        List<String> ids = new ArrayList<>();
                        if (field.equals("military"))
                            ids.add(id + "-" + m.get("unit"));
                        else
                            ids.add(id + "-" + m.get("name"));
                        if (field.equals("military"))
                            list.add(new Tuple2<>((Integer) m.get("unit_id"), ids));
                        else
                            list.add(new Tuple2<>((Integer) m.get("id"), ids));
                    }
                    return list;
                })
                .persist(StorageLevel.MEMORY_AND_DISK())
                //.distinct()
                .reduceByKey((s1, s2) -> {
                    List<String> list = new ArrayList<>(s1);
                    list.addAll(s2);
                    return list;
                });

        Comparator comparator = new ComparatorListString();
        JavaPairRDD<Integer, List<String>> result = rddSortedByValue(pairRDD, comparator);
        return result;
    }

    public static <K, V> JavaPairRDD<K, V> rddSortedByValue(JavaPairRDD<K, V> data, Comparator<V> comparator) {
        JavaPairRDD<V, K> rdd = data.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        rdd = rdd.sortByKey(comparator);
        return rdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    }
}

