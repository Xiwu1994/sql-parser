insert overwrite table secoo_tmp.tmp_table
select t1.order_item_id, t2.user_type, product_name, (t1.pay1 + t1.pay2) as sum_pay
from secoo_fact.fact_order_item_accu_day_full t1
left join secoo_dim.dim_user_basic_p_day_full t2
on t1.user_id = t2.user_id and t2.p_day = '2019-12-03'
inner join secoo_dim.dim_product_basic_p_day t3
on t1.product_id = t3.product_id and t3.p_day = '2019-12-03'
where t1.create_date >= '2019-12-02'
