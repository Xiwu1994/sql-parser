package com.sql.parse.bean;

import lombok.Data;

@Data
public class TableNode {
    private long id;
    private String table;
    private String db;
}