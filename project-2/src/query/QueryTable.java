package query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryTable {
    Map<String, Integer> nameMap;
    List<List<Integer>> rows;

    public QueryTable() {
        this.nameMap = new HashMap<>();
        this.rows = new ArrayList<>();
    }
}
