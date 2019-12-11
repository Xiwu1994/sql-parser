insert overwrite table secoo_tmp.test_sql_lineage_result
select
  t1.user_type,
  product_level,
  (pay1 + pay3 + market_price + sum_discount) as pay4,
  t2.uv + t2.qq_count + t2.email_count
from (
  select
    user_type,
    product_level,
    sum(t1.settle_price_rmb_amt) as pay1,
    sum(refunded_cash_amt + pay2) as pay3
  from secoo_fact.fact_order_item_accu_day_full t1
  left join secoo_dim.dim_user_basic_p_day_full t2
  on t1.user_id = t2.user_id and t2.p_day = '2019-12-02'
  left join (
    select
      product_id,
      sum(pay) as pay2
    from (
      select
        product_id,
        sum(now_price_rmb_amt) as pay
      from secoo_fact.fact_order_item_accu_day_full
      group by product_id

      union all

      select
        id as product_id,
        max(market_price) as pay
      from secoo_ods_mysql.ods_secooerpdb__v_t_product
      where p_day = '2019-12-02'
      group by id
    ) t1
    group by product_id
  ) t3
  on t1.product_id = t3.product_id
  where create_date >= '2019-12-02'
  group by user_type, product_level
) t1
left join (
  select
    user_type,
    count(distinct qq_s) as qq_count,
    count(distinct email_s) as email_count,
    count(distinct user_id) as uv,
    sum(discount) as sum_discount
  from secoo_dim.dim_user_basic_p_day_full t1
  LATERAL VIEW explode(split(t1.qq, ',')) qq_tmp as qq_s
  LATERAL VIEW explode(split(t1.email, ',')) email_tmp as email_s
  where p_day = '2019-12-02'
  group by user_type
) t2
on t1.user_type = t2.user_type
inner join secoo_dim.dim_product_basic_p_day t3
on t1.product_level = t3.product_level and t3.p_day = '2019-12-02'


