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
