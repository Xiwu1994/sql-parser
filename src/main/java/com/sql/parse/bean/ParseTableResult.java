package com.sql.parse.bean;

import lombok.Data;
import java.util.List;

@Data
public class ParseTableResult {
    private String aliasName;
    private String tableName;
    private String dbName;
    private String tableFullName;
    private List<String> columnNameList;
}
