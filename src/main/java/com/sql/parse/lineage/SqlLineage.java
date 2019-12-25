package com.sql.parse.lineage;

import com.sql.parse.bean.*;
import com.sql.parse.dao.DataWarehouseDao;
import com.sql.parse.process.*;
import com.sql.parse.util.MetaCacheUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.log4j.Logger;

import java.util.*;

public class SqlLineage {
    private static Logger logger = Logger.getLogger(SqlLineage.class);
    private DataWarehouseDao dataWarehouseDao = new DataWarehouseDao();

    private Map<String, ParseColumnResult> parseAllColref = new HashMap<>();
    private List<ParseColumnResult> parseColumnResults = new ArrayList();
    private List<ParseTableResult> parseTableResults = new ArrayList();
    private List<ParseJoinResult> parseJoinResults = new ArrayList();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList();
    private List<ParseWithResult> parseWithResults = new ArrayList<>();

    // from tables
    private Set<String> fromTables = new HashSet<>();
    // insert into table name
    private String insertTable;
    // insert into table's column list
    private List<String> insertTableColumns = new ArrayList<>();

    private List<Map<String, ParseColumnResult>> parseQueryResults = new ArrayList<>();
    private Map<String, ParseColumnResult> parseSelectResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseUnionColumnResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
    private Map<String, ParseColumnResult> parseLateralViewResult = new HashMap<>();

    private Map<String, ParseColumnResult> getParseUnionColumnResults() {
        return parseUnionColumnResults;
    }

    private List<Map<String, ParseColumnResult>> getParseQueryResults() {
        return parseQueryResults;
    }

    private void setParseWithResults(List<ParseWithResult> parseWithResults) {
        this.parseWithResults = parseWithResults;
    }

    public void clear() {
        parseWithResults.clear();
        fromTables.clear();
        parseColumnResults.clear();
        parseTableResults.clear();
        parseJoinResults.clear();
        parseSubQueryResults.clear();
        insertTableColumns.clear();
        parseQueryResults.clear();
        parseSelectResults.clear();
        parseUnionColumnResults.clear();
        parseFromResult.clear();
        parseLateralViewResult.clear();
    }

    public ASTNode getASTNode(String sql) throws Exception {
        HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        conf.set("_hive.hdfs.session.path","/tmp");
        conf.set("_hive.local.session.path","/tmp");
        Context context = new Context(conf);
        ParseDriver pd = new ParseDriver();
        ASTNode ast = null;
        try {
            ast = pd.parse(sql, context);
            logger.debug(ast.dump());
        } catch (Exception e) {
            logger.error("process error sql: " + sql);
            logger.error(e);
        }
        return ast;
    }

    public void parseChildASTNode(ASTNode ast) {
        if (ast.getToken() != null && (ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
                ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER)) {
            // later view 比较特殊，需要先处理 第二个节点，再返回处理 第一个节点
            parseASTNode((ASTNode) ast.getChild(1));
            parseASTNode((ASTNode) ast.getChild(0));
        } else if (ast.getToken() != null && (ast.getToken().getType() == HiveParser.TOK_QUERY && ast.getChild(2) != null && ast.getChild(2).getType() == HiveParser.TOK_CTE)) {
            // 判断是否有 with 生成的临时表，优先处理建表语句(SUBQUERY)
            parseASTNode((ASTNode) ast.getChild(2));

            for (int i=0; i<parseSubQueryResults.size(); i++) {
                ParseWithResult parseWithResult = new ParseWithResult();
                parseWithResult.setTableName(parseSubQueryResults.get(i).getAliasName());
                Map<String, ParseColumnResult> parseSubQueryResultTmp = new HashMap<>();
                parseSubQueryResultTmp.putAll(parseSubQueryResults.get(i).getParseSubQueryResults());
                parseWithResult.setParseSubQueryResults(parseSubQueryResultTmp);
                parseWithResults.add(parseWithResult);
            }
            parseSubQueryResults.clear();

            parseASTNode((ASTNode) ast.getChild(0));
            parseASTNode((ASTNode) ast.getChild(1));
        } else {
            int childCount = ast.getChildCount();
            for (int i = 0; i < childCount; i++) {
                parseASTNode((ASTNode) ast.getChild(i));
            }
        }
    }

    public Map<String, ParseColumnResult> genFromColumnData(ASTNode ast) {
        Map<String, ParseColumnResult> fromColumnDataMap = null;
        switch (ast.getToken().getType()) {
            case HiveParser.TOK_SUBQUERY:
                fromColumnDataMap = ProcessSubQueryData.process(parseSubQueryResults);
                parseSubQueryResults.clear();
                break;

            case HiveParser.TOK_TABREF:
                if (parseWithResults.size() > 0) {
                    fromColumnDataMap = ProcessWithData.process(parseWithResults);
//                    parseWithResults.clear();
                } else {
                    fromColumnDataMap = ProcessTabrefData.process(parseTableResults);
                    parseTableResults.clear();
                }
                break;

            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                fromColumnDataMap = ProcessJoinData.process(parseJoinResults);
                parseJoinResults.clear();
                break;

            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                Map<String, ParseColumnResult> lateralViewResultTmp = new HashMap<>();
                lateralViewResultTmp.putAll(parseLateralViewResult);
                parseLateralViewResult.clear();
                fromColumnDataMap = lateralViewResultTmp;
                break;

            default:
                break;
        }
        return fromColumnDataMap;
    }

    public void putInTableDependencies() {
        try {
            for (String dependencyTable : fromTables) {
                dataWarehouseDao.insertTableDependencies(insertTable, dependencyTable);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("insert db error.. Exception: " + e);
        }
    }

    public void putInColumnDependencies(String columnName, Set<String> dependenciesColumns) {
        try {
            for (String dependencyColumn : dependenciesColumns) {
                dataWarehouseDao.insertColumnDependencies(insertTable, columnName, dependencyColumn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("insert db error.. Exception: " + e);
        }
    }

    public void parseCurrentASTNode(ASTNode ast) {
        if(ast.getToken() == null) {
            return;
        }
        switch (ast.getToken().getType()) {
            // CREATE TABLE AS 入库表名
            case HiveParser.TOK_CREATETABLE:
                insertTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                if (parseSelectResults.size() > 0) {
                    System.out.println("\n插入的表名: " + insertTable);
                    MetaCacheUtil.getInstance().init(insertTable);
                    insertTableColumns = MetaCacheUtil.getInstance().getColumnByDBAndTable(insertTable);
                    // 终点 create table as 步骤
                    dataWarehouseDao.deleteColumnDependencies(insertTable);
                    dataWarehouseDao.deleteTableDependencies(insertTable);
                    // 将表的依赖关系入库
                    putInTableDependencies();
                    for (int i=0; i<insertTableColumns.size(); i++) {
                        String createTableColumnName = insertTableColumns.get(i);
                        Set createFromTableColumnSet = null;
                        if (parseAllColref.size() > 0) {
                            if (parseAllColref.containsKey(createTableColumnName)) {
                                createFromTableColumnSet = parseAllColref.get(createTableColumnName).getFromTableColumnSet();
                            }
                        } else {
                            createFromTableColumnSet = parseSelectResults.get(createTableColumnName).getFromTableColumnSet();
                        }
                        System.out.println("字段：" + createTableColumnName + " 依赖字段: " + createFromTableColumnSet);
                        // 将字段的依赖关系入库
                        putInColumnDependencies(insertTable + "." + createTableColumnName, createFromTableColumnSet);
                    }
                    parseAllColref.clear();
                }
                break;

            // INSERT 入库表名
            case HiveParser.TOK_TAB:
                insertTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                System.out.println("\n插入的表名: " + insertTable);
                MetaCacheUtil.getInstance().init(insertTable);
                insertTableColumns = MetaCacheUtil.getInstance().getColumnByDBAndTable(insertTable);
                break;

            case HiveParser.TOK_UNIONALL:
                // from TOK_QUERY or TOK_UNIONALL
                // to TOK_SUBQUERY or TOK_UNIONALL
                Map<String, ParseColumnResult> newParseColumnResultMap = new HashMap<>();

                Map<String, ParseColumnResult> parseColumnResultMap = parseQueryResults.get(0);

                for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                    String columnAliasName = entry.getKey();
                    ParseColumnResult parseColumnResult = entry.getValue();
                    // (TOK_QUERY) UNION ALL (TOK_QUERY)
                    if (parseQueryResults.size() == 2) {
                        Set<String> otherUnionFromColumnSet = parseQueryResults.get(1).get(columnAliasName).getFromTableColumnSet();
                        parseColumnResult.getFromTableColumnSet().addAll(otherUnionFromColumnSet);
                        newParseColumnResultMap.put(columnAliasName, parseColumnResult);
                    }
                    // (TOK_UNIONALL) UNION ALL (TOK_QUERY)
                    if (parseUnionColumnResults.size() > 0) {
                        Set<String> otherUnionFromColumnSet = parseUnionColumnResults.get(columnAliasName).getFromTableColumnSet();
                        parseColumnResult.getFromTableColumnSet().addAll(otherUnionFromColumnSet);
                        newParseColumnResultMap.put(columnAliasName, parseColumnResult);
                    }
                }
                parseUnionColumnResults.putAll(newParseColumnResultMap);
                parseQueryResults.clear();
                break;

            case HiveParser.TOK_QUERY:
                // from TOK_INSERT
                // to TOK_SUBQUERY or TOK_UNIONALL
                Map<String, ParseColumnResult> queryColumnMapTmp = new HashMap<>();
                queryColumnMapTmp.putAll(parseSelectResults);
                parseQueryResults.add(queryColumnMapTmp);
                parseSelectResults.clear();
                break;

            case HiveParser.TOK_INSERT:
                // from TOK_SELEXPR
                // to TOK_QUERY
                Map<String, ParseColumnResult> selectResultsTmp = new HashMap();

                if (insertTableColumns.size() > 0) {
                    // 终点： insert into table 步骤
                    dataWarehouseDao.deleteColumnDependencies(insertTable);
                    dataWarehouseDao.deleteTableDependencies(insertTable);
                    // 将表的依赖关系入库
                    putInTableDependencies();

                    for (int i = 0; i < insertTableColumns.size(); i++) {
                        String insertTableColumnName = insertTableColumns.get(i);
                        Set<String> insertFromTableColumnSet = null;
                        if (parseAllColref.size() > 0) {
                            // 判断是否是 select * 入库方式
                            // TODO: parseFromResult 应该改成List类型，因为select * 是有顺序的。但是改动太大了..
                            // TODO: insert table1 select * from table2  如果table1和table2的字段名不一样，字段血缘 解析不出
                            if (parseAllColref.containsKey(insertTableColumnName)) {
                                insertFromTableColumnSet = parseAllColref.get(insertTableColumnName).getFromTableColumnSet();
                            }
                        } else {
                            insertFromTableColumnSet = parseColumnResults.get(i).getFromTableColumnSet();
                        }
                        System.out.println("字段：" + insertTableColumnName + " 依赖字段: " + insertFromTableColumnSet);
                        // 将字段的依赖关系入库
                        putInColumnDependencies(insertTable + "." + insertTableColumnName, insertFromTableColumnSet);
                    }
                    parseAllColref.clear();
                    parseColumnResults.clear();
                    insertTableColumns.clear();
                } else {
                    for (int i = 0; i < parseColumnResults.size(); i++) {
                        selectResultsTmp.put(parseColumnResults.get(i).getAliasName(), parseColumnResults.get(i));
                    }
                    parseColumnResults.clear();
                    parseSelectResults.putAll(selectResultsTmp);
                    logger.debug("TOK_INSERT: " + selectResultsTmp);
                }
                break;

            // TOK_LATERAL_VIEW 行转列
            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                // from TOK_SUBQUERY or TOK_TABREF or TOK_JOIN
                // to TOK_FROM
                parseLateralViewResult = genFromColumnData((ASTNode) ast.getChild(1));

                // 把 lateral view 生成的字段也 放进fromTableColumnMap结构里
                ProcessTokSelexpr laterViewTokSelexpr = new ProcessTokSelexpr();
                laterViewTokSelexpr.setParseFromResult(parseLateralViewResult);
                ParseColumnResult laterViewParseColumnResult = laterViewTokSelexpr.process((ASTNode) ast.getChild(0).getChild(0));

                // 判断laterview 是否有别名
                String laterViewColumnPrefix;
                if (ast.getChild(0).getChild(0).getChild(2) != null && ast.getChild(0).getChild(0).getChild(2).getType() == HiveParser.TOK_TABALIAS) {
                    String latervalViewAliasName = ast.getChild(0).getChild(0).getChild(2).getChild(0).getText();
                    laterViewColumnPrefix = latervalViewAliasName + ".";
                } else {
                    laterViewColumnPrefix = ".";
                }
                parseLateralViewResult.put(laterViewColumnPrefix + laterViewParseColumnResult.getAliasName(), laterViewParseColumnResult);

                parseColumnResults.clear();

                break;

            // TOK_FROM
            case HiveParser.TOK_FROM:
                // from TOK_SUBQUERY or TOK_TABREF or TOK_JOIN or TOK_LATERAL_VIEW
                // to TOK_SELEXPR
                parseFromResult = genFromColumnData((ASTNode) ast.getChild(0));
                logger.debug("TOK_FROM: " + parseFromResult);
                break;

            // TOK_JOIN
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                // from TOK_JOIN or TOK_FROM or TOK_SUBQUERY
                // to TOK_JOIN or TOK_FROM to TOK_SUBQUERY
                ParseJoinResult parseJoinResult = new ParseJoinResult();

                List<ParseTableResult> tableResults = new ArrayList<>();
                tableResults.addAll(parseTableResults);
                parseTableResults.clear();
                parseJoinResult.setParseTableResults(tableResults);

                List<ParseJoinResult> joinResults = new ArrayList<>();
                joinResults.addAll(parseJoinResults);
                parseJoinResults.clear();
                parseJoinResult.setParseJoinResults(joinResults);

                List<ParseSubQueryResult> subQueryResults = new ArrayList<>();
                subQueryResults.addAll(parseSubQueryResults);
                parseSubQueryResults.clear();
                parseJoinResult.setParseSubQueryResults(subQueryResults);

                List<ParseWithResult> withResults = new ArrayList<>();
                withResults.addAll(parseWithResults);
//                parseWithResults.clear();
                parseJoinResult.setParseWithResults(withResults);

                logger.debug("TOK_JOIN: " + parseJoinResult);

                parseJoinResults.add(parseJoinResult);

                break;

            // TOK_TABREF
            case HiveParser.TOK_TABREF:
                String fromTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                Boolean isWithTable = Boolean.FALSE;
                for (int i=0; i<parseWithResults.size(); i++) {
                    if (parseWithResults.get(i).getTableName().equals(fromTableName)) {
                        isWithTable = Boolean.TRUE;
                        if (ast.getChild(1) != null) {
                            String withTableAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                            parseWithResults.get(i).setAliasName(withTableAlias);
                        }
                        break;
                    }
                }
                if (! isWithTable) {
                    fromTables.add(fromTableName);
                    // to TOK_FROM or TOK_JOIN
                    ParseTableResult parseTableResult = ProcessTokTabref.process(ast);
                    logger.debug("TOK_TABREF: " + parseTableResult);
                    parseTableResults.add(parseTableResult);
                }
                break;

            // SELECT
            case HiveParser.TOK_SELEXPR:
                // from TOK_FROM
                // to TOK_INSERT
                if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                    // 判断是否是 select *
                    for(Map.Entry<String, ParseColumnResult> entry : parseFromResult.entrySet()){
                        ParseColumnResult parseColumnResult = entry.getValue();
                        parseAllColref.put(parseColumnResult.getAliasName(), parseColumnResult);
                    }
                } else {
                    ProcessTokSelexpr processTokSelexpr = new ProcessTokSelexpr();
                    processTokSelexpr.setParseFromResult(parseFromResult);
                    ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);
                    logger.debug("TOK_SELEXPR: " + parseColumnResult);
                    parseColumnResults.add(parseColumnResult);
                }
                break;
            default:
                break;
        }
    }

    public void parseASTNode(ASTNode ast) {
        if (ast == null) {
            return;
        }

        if (ast.getToken() != null && ast.getToken().getType() == HiveParser.TOK_SUBQUERY) {
            // TOK_SUBQUERY 子查询
            // from TOK_UNIONALL or TOK_QUERY
            // to TOK_FROM or TOK_JOIN
            SqlLineage sqlLineage = new SqlLineage();
            sqlLineage.setParseWithResults(parseWithResults);
            sqlLineage.parseASTNode((ASTNode) ast.getChild(0));

            Map<String, ParseColumnResult> subQueryColumnMap;
            if (sqlLineage.getParseUnionColumnResults().size() > 0) {
                // 有UNION ALL 操作
                subQueryColumnMap = sqlLineage.getParseUnionColumnResults();
            } else {
                subQueryColumnMap = sqlLineage.getParseQueryResults().get(0);
            }
            ParseSubQueryResult parseSubQueryResult = new ParseSubQueryResult();
            String subQueryAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            parseSubQueryResult.setAliasName(subQueryAliasName);

            Map<String, ParseColumnResult> selectResults = new HashMap<>();
            selectResults.putAll(subQueryColumnMap);
            parseSubQueryResult.setParseSubQueryResults(selectResults);
            logger.debug("TOK_SUBQUERY: " + parseSubQueryResult);
            parseSubQueryResults.add(parseSubQueryResult);
        } else {
            // 非 TOK_SUBQUERY 子查询
            // 递归处理，优先处理子节点
            parseChildASTNode(ast);
            // 子节点都处理完后，处理当前节点
            parseCurrentASTNode(ast);
        }
    }

    public void parse(String sql) throws Exception {
        clear();
        ASTNode ast = getASTNode(sql);
        parseASTNode(ast);
    }
}
