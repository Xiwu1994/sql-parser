package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import com.sql.parse.bean.ParseWithResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessWithData {
    public static Map<String, ParseColumnResult> process(List<ParseWithResult> withResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < withResults.size(); i++) {
            ParseWithResult parseWithResult = withResults.get(i);
            String subQueryAliasName = parseWithResult.getAliasName();
            Map<String, ParseColumnResult> parseColumnResultMap = parseWithResult.getParseSubQueryResults();

            for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                String columnAliasName = entry.getKey();
                ParseColumnResult parseColumnResult = entry.getValue();
                parseFromResult.put(subQueryAliasName + "." + columnAliasName, parseColumnResult);
            }
        }
        return parseFromResult;
    }
}

