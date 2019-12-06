package com.sql.parse.bean;

public class TableNode {
    private long id;
    private String table;
    private String db;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }


}