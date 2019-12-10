package com.sql.parse;

import com.sql.parse.lineage.SqlLineage;
import com.sql.parse.util.FileUtil;


public class App {
    public static void main(String[] args) throws Exception {
        String sql = FileUtil.read("/Users/liebaomac/IdeaProjects/sql-parser/src/main/java/com/sql/parse/test.sql");
        SqlLineage sqlLineage = new SqlLineage();
        sqlLineage.parse(sql);
    }
}
