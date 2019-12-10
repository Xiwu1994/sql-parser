package com.sql.parse.bean;

import lombok.Data;

@Data
public class ColumnNode {
    private long id;
    private String column;
    private long tableId;
    private String table;
    private String db;
}
