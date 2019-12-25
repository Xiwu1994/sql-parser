package com.sql.parse.process;

import com.sql.parse.bean.*;

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
                parseFromResult.putAll(ProcessTabrefData.process(parseTableResults));
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
            // 处理 WITH
            if (parseJoinResult.getParseWithResults() != null) {
                List<ParseWithResult> parseWiteResults = parseJoinResult.getParseWithResults();
                parseFromResult.putAll(ProcessWithData.process(parseWiteResults));
            }
        }
        return parseFromResult;
    }
}
