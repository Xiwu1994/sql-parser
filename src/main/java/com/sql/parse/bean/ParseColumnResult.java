package com.sql.parse.bean;

import lombok.Data;

import java.util.Set;

@Data
public class ParseColumnResult {
    private String aliasName;
    private int index;
    private Set<String> fromTableColumnSet;
}
