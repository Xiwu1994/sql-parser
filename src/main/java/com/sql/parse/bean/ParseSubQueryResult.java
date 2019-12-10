package com.sql.parse.bean;

import lombok.Data;

import java.util.Map;

@Data
public class ParseSubQueryResult {
    private String aliasName;
    private Map<String, ParseColumnResult> parseSubQueryResults;
}
