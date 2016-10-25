package services.comparators;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Владимир on 21.10.2016.
 */
public class ComparatorListString implements Comparator<List<String>>, Serializable {
    @Override
    public int compare(List<String> o1, List<String> o2) {
        return o2.size() - o1.size();
    }
}
