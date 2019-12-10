package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import com.sql.parse.bean.ParseSubQueryResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessSubQueryData {
    public static Map<String, ParseColumnResult> process(List<ParseSubQueryResult> subQueryResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < subQueryResults.size(); i++) {
            ParseSubQueryResult parseSubQueryResult = subQueryResults.get(i);
            String subQueryAliasName = parseSubQueryResult.getAliasName();
            Map<String, ParseColumnResult> parseColumnResultMap = parseSubQueryResult.getParseSubQueryResults();

            for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                String columnAliasName = entry.getKey();
                ParseColumnResult parseColumnResult = entry.getValue();
                parseFromResult.put(subQueryAliasName + "." + columnAliasName, parseColumnResult);
            }
        }
        return parseFromResult;
    }
}
