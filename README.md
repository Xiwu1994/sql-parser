### 字段之间血缘
main class:  com.sql.parse.App

***

线上执行例子:
``` bash
pcsjob@node4018:/data/soft/sql-parser$ java -jar sql-parser-1.0-SNAPSHOT-jar-with-dependencies.jar /data/soft/sql-parser/app.properties /data/soft/data-warehouse/sql/secoo_dim/dim_product_basic_p_day.sql

插入的表名: secoo_dim.dim_product_basic_p_day
字段：product_id 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.id]
字段：product_name 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.name]
字段：product_model 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.model]
字段：product_level 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.level]
字段：product_main_id 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.main_id]
字段：is_major 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.is_major]
字段：settle_price 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.settle_price]
字段：market_price 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.market_price]
字段：secoo_price 依赖字段: [secoo_ods_mysql.ods_secooerpdb__v_t_product.secoo_price]
...
```

```sql
create table metadata_table_dependencies_mapping (
  `id` bigint(20) not null auto_increment comment "主键ID",
  `table_name` varchar(255) not null comment "表名",
  `dependency_table_name` varchar(255) not null comment "依赖表名",
  primary key (`id`),
  key `idx_table_name` (`table_name`)
) engine=innodb default charset=utf8mb4 comment='元数据-表依赖关系表';


create table metadata_column_dependencies_mapping (
  `id` bigint(20) not null auto_increment comment "主键ID",
  `table_name` varchar(255) not null comment "表名",
  `column_name` varchar(255) not null comment "字段名（库名.表名.字段名）",
  `dependency_column_name` varchar(255) not null comment "依赖字段名（库名.表名.字段名）",
  primary key (`id`),
  key `idx_column_name` (`column_name`),
  key `idx_table_name` (`table_name`)
) engine=innodb default charset=utf8mb4 comment='元数据-字段依赖关系表';


CREATE TABLE `metadata_table_join_on_relation` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `left_table` varchar(255) NOT NULL COMMENT '表1',
  `right_table` varchar(255) NOT NULL COMMENT '表2',
  `left_columns` varchar(255) NOT NULL COMMENT '表1 join 字段',
  `right_columns` varchar(255) NOT NULL COMMENT '表2 join 字段',
  `file_path` varchar(255) DEFAULT NULL COMMENT '文件名',
  PRIMARY KEY (`id`),
  KEY `idx_table_name` (`left_table`, `right_table`),
  KEY `idx_file_path` (`file_path`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='元数据-JOIN关系表';
```
