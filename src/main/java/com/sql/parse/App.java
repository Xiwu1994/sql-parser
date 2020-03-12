package com.sql.parse;

import com.sql.parse.lineage.SqlLineage;
import com.sql.parse.util.CheckUtil;
import com.sql.parse.util.FileUtil;
import com.sql.parse.util.PropertyFileUtil;
import com.sql.parse.util.StringUtil;
import org.apache.log4j.Logger;

public class App {
    private static Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) throws Exception {
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
        sqlLineage.setFilePath(sqlPath);
        sqlLineage.deleteJoinRelation();
        for (String sql: sqlList.split("(?<!\\\\);")) {
            sql = StringUtil.subVariable(sql).replace("\\\"", " ");
            String sqlTrim = sql.toLowerCase().trim().replace("/*+ mapjoin", "--");
            if (sqlTrim.startsWith("set") || sqlTrim.startsWith("add") || sqlTrim.startsWith("drop")||
                   sqlTrim.startsWith("load") || CheckUtil.isEmpty(sqlTrim)) {
                continue;
            }
            if (sqlTrim.contains("select")) {
                sqlLineage.parse(sqlTrim);
            }
        }
    }
}
