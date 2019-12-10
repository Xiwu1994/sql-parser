package com.sql.parse.bean;

import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class ParseColumnResult {
    private String aliasName;
    private Set<String> fromTableColumnSet;
}
