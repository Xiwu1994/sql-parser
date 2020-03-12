package com.sql.parse;


import com.sql.parse.lineage.SqlLineage;
import com.sql.parse.util.*;
import org.apache.log4j.Logger;

import java.util.LinkedList;


public class TraverseFolderApp {
    private static Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            logger.error("need properties and sql file");
            System.exit(1);
        }
        String propertiesPath = args[0];
        String sqlPath = args[1];

        PropertyFileUtil.init(propertiesPath);

        LinkedList filePathList = TraverseFolder.traverseFolder(sqlPath);
        for (Object filePath : filePathList) {
            String filePathStr = filePath.toString();
            logger.info("process sql path: " + filePathStr);
            String sqlList = FileUtil.read(filePathStr);

            SqlLineage sqlLineage = new SqlLineage();
            sqlLineage.setFilePath(filePathStr);
            sqlLineage.deleteFilePathDbData();
            for (String sql : sqlList.split("(?<!\\\\);")) {
                sql = StringUtil.subVariable(sql).replace("\\\"", " ");
                String sqlTrim = sql.toLowerCase().trim().replace("/*+ mapjoin", "--");
                if (sqlTrim.startsWith("set") || sqlTrim.startsWith("add") || sqlTrim.startsWith("drop") ||
                        sqlTrim.startsWith("load") || CheckUtil.isEmpty(sqlTrim)) {
                    continue;
                }
                if (sqlTrim.contains("select")) {
                    sqlLineage.parse(sqlTrim);
                }
            }
        }
    }
}
