package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import com.sql.parse.bean.ParseJoinResult;
import com.sql.parse.bean.ParseSubQueryResult;
import com.sql.parse.bean.ParseTableResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessJoinData {
    public static Map<String, ParseColumnResult> process(List<ParseJoinResult> joinResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < joinResults.size(); i++) {
            ParseJoinResult parseJoinResult = joinResults.get(i);
            // 处理 FROM
            if (parseJoinResult.getParseTableResults() != null) {
                List<ParseTableResult> parseTableResults = parseJoinResult.getParseTableResults();
                parseFromResult.putAll(ProcessFromData.process(parseTableResults));
            }
            // 递归处理JOIN
            if (parseJoinResult.getParseJoinResults() != null) {
                parseFromResult.putAll(ProcessJoinData.process(parseJoinResult.getParseJoinResults()));
            }
            // 处理 SUBQUERY
            if (parseJoinResult.getParseSubQueryResults() != null) {
                List<ParseSubQueryResult> parseSubQueryResults = parseJoinResult.getParseSubQueryResults();
                parseFromResult.putAll(ProcessSubQueryData.process(parseSubQueryResults));
            }
        }
        return parseFromResult;
    }
}
