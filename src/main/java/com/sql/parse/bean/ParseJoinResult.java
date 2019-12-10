package com.sql.parse.bean;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ParseJoinResult {
    private List<ParseTableResult> parseTableResults;
    private List<ParseJoinResult> parseJoinResults;
    private List<ParseSubQueryResult> parseSubQueryResults;
}
