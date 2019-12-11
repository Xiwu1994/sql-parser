package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import com.sql.parse.bean.ParseTableResult;

import java.util.*;

public class ProcessTabrefData {
    public static Map<String, ParseColumnResult> process(List<ParseTableResult> fromResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < fromResults.size(); i++) {
            ParseTableResult parseTableResult = fromResults.get(i);
            String tableAliasName = parseTableResult.getAliasName();
            String tableFullName = parseTableResult.getTableFullName();
            List<String> columnNameList = parseTableResult.getColumnNameList();
            for (int j = 0; j < columnNameList.size(); j++) {
                String columnName = columnNameList.get(j);
                ParseColumnResult parseColumnResult = new ParseColumnResult();
                parseColumnResult.setAliasName(columnName);
                Set<String> fromTableColumnSet = new TreeSet<>();
                fromTableColumnSet.add(tableFullName + "." + columnName);
                parseColumnResult.setFromTableColumnSet(fromTableColumnSet);
                parseFromResult.put(tableAliasName + "." + columnName, parseColumnResult);
            }
        }
        return parseFromResult;
    }
}
