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

    private List<ParseColumnResult> parseColumnResults = new ArrayList();
    private List<ParseTableResult> parseTableResults = new ArrayList();
    private List<ParseJoinResult> parseJoinResults = new ArrayList();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList();

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

    public void clear() {
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
                fromColumnDataMap = ProcessTabrefData.process(parseTableResults);
                parseTableResults.clear();
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

    public void putInColumnDependencies(String columnName, Set<String> dependenciesColumns) {
        try {
            for (String dependencyColumn : dependenciesColumns) {
                dataWarehouseDao.insertColumnDependencies(columnName, dependencyColumn);
            }
        } catch (Exception e) {
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
                    for (int i=0; i<insertTableColumns.size(); i++) {
                        String createTableColumnName = insertTableColumns.get(i);
                        Set createFromTableColumnSet = parseSelectResults.get(createTableColumnName).getFromTableColumnSet();
                        System.out.println("字段：" + createTableColumnName + " 依赖字段: " + createFromTableColumnSet);
                        putInColumnDependencies(insertTable + "." + createTableColumnName, createFromTableColumnSet);
                    }
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
                // from TOK_QUERY
                // to TOK_SUBQUERY
                Map<String, ParseColumnResult> newParseColumnResultMap = new HashMap<>();

                Map<String, ParseColumnResult> parseColumnResultMap = parseQueryResults.get(0);

                for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                    String columnAliasName = entry.getKey();
                    ParseColumnResult parseColumnResult = entry.getValue();

                    for (int i=1; i<parseQueryResults.size(); i++) {
                        Set<String> otherUnionFromColumnSet = parseQueryResults.get(i).get(columnAliasName).getFromTableColumnSet();
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
                    for (int i = 0; i < parseColumnResults.size(); i++) {
                        String insertTableColumnName = insertTableColumns.get(i);
                        Set<String> insertFromTableColumnSet = parseColumnResults.get(i).getFromTableColumnSet();
                        System.out.println("字段：" + insertTableColumnName + " 依赖字段: " + insertFromTableColumnSet);
                        putInColumnDependencies(insertTable + "." + insertTableColumnName, insertFromTableColumnSet);
                    }
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

                logger.debug("TOK_JOIN: " + parseJoinResult);

                parseJoinResults.add(parseJoinResult);

                break;

            // TOK_TABREF
            case HiveParser.TOK_TABREF:
                // to TOK_FROM or TOK_JOIN
                ParseTableResult parseTableResult = ProcessTokTabref.process(ast);
                logger.debug("TOK_TABREF: " + parseTableResult);

                parseTableResults.add(parseTableResult);
                break;

            // SELECT
            case HiveParser.TOK_SELEXPR:
                // from TOK_FROM
                // to TOK_INSERT
                ProcessTokSelexpr processTokSelexpr = new ProcessTokSelexpr();
                processTokSelexpr.setParseFromResult(parseFromResult);
                ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);
                logger.debug("TOK_SELEXPR: " + parseColumnResult);
                parseColumnResults.add(parseColumnResult);
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
