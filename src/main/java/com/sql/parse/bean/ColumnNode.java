package com.sql.parse.bean;

public class ColumnNode {
    private long id;
    private String column;
    private long tableId;
    private String table;
    private String db;

    public void setId(long id) {
        this.id = id;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public long getId() {
        return id;
    }

    public String getColumn() {
        return column;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTable() {
        return table;
    }

    public String getDb() {
        return db;
    }
}
