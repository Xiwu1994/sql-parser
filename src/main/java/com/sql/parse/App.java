package com.sql.parse;

import com.sql.parse.lineage.SqlLineage;
import com.sql.parse.util.CheckUtil;
import com.sql.parse.util.FileUtil;
import com.sql.parse.util.PropertyFileUtil;
import org.apache.log4j.Logger;


public class App {
    private static Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        // TODO 添加入库 neo4j ?
        if (args.length < 2) {
            logger.error("need properties and sql file");
            System.exit(1);
        }
        String propertiesPath = args[0];
        String sqlPath = args[1];
        logger.info("process sql path: " + sqlPath);
        PropertyFileUtil.init(propertiesPath);
        String sqlList = FileUtil.read(sqlPath);

        SqlLineage sqlLineage = new SqlLineage();
        for (String sql: sqlList.split("(?<!\\\\);")) {
            sql = sql.replace("${", "'").replace("}", "'").replace("\\\"", " ");
            String sqlTrim = sql.toLowerCase().trim();
            if (sqlTrim.startsWith("set") || sqlTrim.startsWith("add") || sqlTrim.startsWith("drop")||
                   sqlTrim.startsWith("LOAD") || CheckUtil.isEmpty(sqlTrim)) {
                continue;
            }
            if (sqlTrim.contains("select")) {
                sqlLineage.parse(sqlTrim);
            }
        }
    }
}
