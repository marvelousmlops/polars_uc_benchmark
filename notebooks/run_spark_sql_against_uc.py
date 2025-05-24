# Databricks notebook source
# run on Databricks
%pip install -r ../requirements.txt

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent / 'src'))

scale = dbutils.widgets.get("scale")

# COMMAND ----------

from polars_pyspark_uc.config import ProjectConfig
from databricks.connect import DatabricksSession
import time

spark = DatabricksSession.builder.getOrCreate()
config = ProjectConfig.from_yaml(config_path="../project_config.yml")
CATALOG = config.catalog
SCHEMA = f"{config.schema}_scale_{scale}"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {SCHEMA}")

ts = time.time()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #1
# MAGIC     select
# MAGIC         l_returnflag,
# MAGIC         l_linestatus,
# MAGIC         sum(l_quantity) as sum_qty,
# MAGIC         sum(l_extendedprice) as sum_base_price,
# MAGIC         sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
# MAGIC         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
# MAGIC         avg(l_quantity) as avg_qty,
# MAGIC         avg(l_extendedprice) as avg_price,
# MAGIC         avg(l_discount) as avg_disc,
# MAGIC         count(*) as count_order
# MAGIC     from
# MAGIC         lineitem
# MAGIC     where
# MAGIC         date(l_shipdate) <= date('1998-09-02')
# MAGIC     group by
# MAGIC         l_returnflag,
# MAGIC         l_linestatus
# MAGIC     order by
# MAGIC         l_returnflag,
# MAGIC         l_linestatus

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #2
# MAGIC     select
# MAGIC         s_acctbal,
# MAGIC         s_name,
# MAGIC         n_name,
# MAGIC         p_partkey,
# MAGIC         p_mfgr,
# MAGIC         s_address,
# MAGIC         s_phone,
# MAGIC         s_comment
# MAGIC     from
# MAGIC         part,
# MAGIC         supplier,
# MAGIC         partsupp,
# MAGIC         nation,
# MAGIC         region
# MAGIC     where
# MAGIC         p_partkey = ps_partkey
# MAGIC         and s_suppkey = ps_suppkey
# MAGIC         and p_size = 15
# MAGIC         and p_type like '%BRASS'
# MAGIC         and s_nationkey = n_nationkey
# MAGIC         and n_regionkey = r_regionkey
# MAGIC         and r_name = 'EUROPE'
# MAGIC         and ps_supplycost = (
# MAGIC             select
# MAGIC                 min(ps_supplycost)
# MAGIC             from
# MAGIC                 partsupp,
# MAGIC                 supplier,
# MAGIC                 nation,
# MAGIC                 region
# MAGIC             where
# MAGIC                 p_partkey = ps_partkey
# MAGIC                 and s_suppkey = ps_suppkey
# MAGIC                 and s_nationkey = n_nationkey
# MAGIC                 and n_regionkey = r_regionkey
# MAGIC                 and r_name = 'EUROPE'
# MAGIC         )
# MAGIC     order by
# MAGIC         s_acctbal desc,
# MAGIC         n_name,
# MAGIC         s_name,
# MAGIC         p_partkey
# MAGIC     limit 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #3
# MAGIC     select
# MAGIC         l_orderkey,
# MAGIC         sum(l_extendedprice * (1 - l_discount)) as revenue,
# MAGIC         date(o_orderdate),
# MAGIC         o_shippriority
# MAGIC     from
# MAGIC         customer,
# MAGIC         orders,
# MAGIC         lineitem
# MAGIC     where
# MAGIC         c_mktsegment = 'BUILDING'
# MAGIC         and c_custkey = o_custkey
# MAGIC         and l_orderkey = o_orderkey
# MAGIC         and o_orderdate < date '1995-03-15'
# MAGIC         and l_shipdate > date '1995-03-15'
# MAGIC     group by
# MAGIC         l_orderkey,
# MAGIC         o_orderdate,
# MAGIC         o_shippriority
# MAGIC     order by
# MAGIC         revenue desc,
# MAGIC         o_orderdate
# MAGIC     limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #4
# MAGIC     select
# MAGIC         o_orderpriority,
# MAGIC         count(*) as order_count
# MAGIC     from
# MAGIC         orders
# MAGIC     where
# MAGIC         o_orderdate >= date '1993-07-01'
# MAGIC         and o_orderdate < date '1993-07-01' + interval '3' month
# MAGIC         and exists (
# MAGIC             select
# MAGIC                 *
# MAGIC             from
# MAGIC                 lineitem
# MAGIC             where
# MAGIC                 l_orderkey = o_orderkey
# MAGIC                 and l_commitdate < l_receiptdate
# MAGIC         )
# MAGIC     group by
# MAGIC         o_orderpriority
# MAGIC     order by
# MAGIC         o_orderpriority

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #5
# MAGIC     select
# MAGIC         n_name,
# MAGIC         sum(l_extendedprice * (1 - l_discount)) as revenue
# MAGIC     from
# MAGIC         customer,
# MAGIC         orders,
# MAGIC         lineitem,
# MAGIC         supplier,
# MAGIC         nation,
# MAGIC         region
# MAGIC     where
# MAGIC         c_custkey = o_custkey
# MAGIC         and l_orderkey = o_orderkey
# MAGIC         and l_suppkey = s_suppkey
# MAGIC         and c_nationkey = s_nationkey
# MAGIC         and s_nationkey = n_nationkey
# MAGIC         and n_regionkey = r_regionkey
# MAGIC         and r_name = 'ASIA'
# MAGIC         and o_orderdate >= date '1994-01-01'
# MAGIC         and o_orderdate < date '1994-01-01' + interval '1' year
# MAGIC     group by
# MAGIC         n_name
# MAGIC     order by
# MAGIC         revenue desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #6
# MAGIC     select
# MAGIC         sum(l_extendedprice * l_discount) as revenue
# MAGIC     from
# MAGIC         lineitem
# MAGIC     where
# MAGIC         l_shipdate >= date '1994-01-01'
# MAGIC         and l_shipdate < date '1994-01-01' + interval '1' year
# MAGIC         and l_discount between .06 - 0.01 and .06 + 0.01
# MAGIC         and l_quantity < 24

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #7
# MAGIC     select
# MAGIC         supp_nation,
# MAGIC         cust_nation,
# MAGIC         l_year,
# MAGIC         sum(volume) as revenue
# MAGIC     from
# MAGIC         (
# MAGIC             select
# MAGIC                 n1.n_name as supp_nation,
# MAGIC                 n2.n_name as cust_nation,
# MAGIC                 year(l_shipdate) as l_year,
# MAGIC                 l_extendedprice * (1 - l_discount) as volume
# MAGIC             from
# MAGIC                 supplier,
# MAGIC                 lineitem,
# MAGIC                 orders,
# MAGIC                 customer,
# MAGIC                 nation n1,
# MAGIC                 nation n2
# MAGIC             where
# MAGIC                 s_suppkey = l_suppkey
# MAGIC                 and o_orderkey = l_orderkey
# MAGIC                 and c_custkey = o_custkey
# MAGIC                 and s_nationkey = n1.n_nationkey
# MAGIC                 and c_nationkey = n2.n_nationkey
# MAGIC                 and (
# MAGIC                     (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
# MAGIC                     or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
# MAGIC                 )
# MAGIC                 and date(l_shipdate) between date '1995-01-01' and date '1996-12-31'
# MAGIC         ) as shipping
# MAGIC     group by
# MAGIC         supp_nation,
# MAGIC         cust_nation,
# MAGIC         l_year
# MAGIC     order by
# MAGIC         supp_nation,
# MAGIC         cust_nation,
# MAGIC         l_year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #8
# MAGIC     select
# MAGIC         o_year,
# MAGIC         round(
# MAGIC             sum(case
# MAGIC                 when nation = 'BRAZIL' then volume
# MAGIC                 else 0
# MAGIC             end) / sum(volume)
# MAGIC         , 2) as mkt_share
# MAGIC     from
# MAGIC         (
# MAGIC             select
# MAGIC                 extract(year from o_orderdate) as o_year,
# MAGIC                 l_extendedprice * (1 - l_discount) as volume,
# MAGIC                 n2.n_name as nation
# MAGIC             from
# MAGIC                 part,
# MAGIC                 supplier,
# MAGIC                 lineitem,
# MAGIC                 orders,
# MAGIC                 customer,
# MAGIC                 nation n1,
# MAGIC                 nation n2,
# MAGIC                 region
# MAGIC             where
# MAGIC                 p_partkey = l_partkey
# MAGIC                 and s_suppkey = l_suppkey
# MAGIC                 and l_orderkey = o_orderkey
# MAGIC                 and o_custkey = c_custkey
# MAGIC                 and c_nationkey = n1.n_nationkey
# MAGIC                 and n1.n_regionkey = r_regionkey
# MAGIC                 and r_name = 'AMERICA'
# MAGIC                 and s_nationkey = n2.n_nationkey
# MAGIC                 and o_orderdate between date '1995-01-01' and date '1996-12-31'
# MAGIC                 and p_type = 'ECONOMY ANODIZED STEEL'
# MAGIC         ) as all_nations
# MAGIC     group by
# MAGIC         o_year
# MAGIC     order by
# MAGIC         o_year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #9
# MAGIC     select
# MAGIC         nation,
# MAGIC         o_year,
# MAGIC         round(sum(amount), 2) as sum_profit
# MAGIC     from
# MAGIC         (
# MAGIC             select
# MAGIC                 n_name as nation,
# MAGIC                 year(o_orderdate) as o_year,
# MAGIC                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
# MAGIC             from
# MAGIC                 part,
# MAGIC                 supplier,
# MAGIC                 lineitem,
# MAGIC                 partsupp,
# MAGIC                 orders,
# MAGIC                 nation
# MAGIC             where
# MAGIC                 s_suppkey = l_suppkey
# MAGIC                 and ps_suppkey = l_suppkey
# MAGIC                 and ps_partkey = l_partkey
# MAGIC                 and p_partkey = l_partkey
# MAGIC                 and o_orderkey = l_orderkey
# MAGIC                 and s_nationkey = n_nationkey
# MAGIC                 and p_name like '%green%'
# MAGIC         ) as profit
# MAGIC     group by
# MAGIC         nation,
# MAGIC         o_year
# MAGIC     order by
# MAGIC         nation,
# MAGIC         o_year desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #10
# MAGIC    select
# MAGIC         c_custkey,
# MAGIC         c_name,
# MAGIC         round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
# MAGIC         c_acctbal,
# MAGIC         n_name,
# MAGIC         c_address,
# MAGIC         c_phone,
# MAGIC         c_comment
# MAGIC     from
# MAGIC         customer,
# MAGIC         orders,
# MAGIC         lineitem,
# MAGIC         nation
# MAGIC     where
# MAGIC         c_custkey = o_custkey
# MAGIC         and l_orderkey = o_orderkey
# MAGIC         and o_orderdate >= date '1993-10-01'
# MAGIC         and o_orderdate < date '1993-10-01' + interval '3' month
# MAGIC         and l_returnflag = 'R'
# MAGIC         and c_nationkey = n_nationkey
# MAGIC     group by
# MAGIC         c_custkey,
# MAGIC         c_name,
# MAGIC         c_acctbal,
# MAGIC         c_phone,
# MAGIC         n_name,
# MAGIC         c_address,
# MAGIC         c_comment
# MAGIC     order by
# MAGIC         revenue desc
# MAGIC     limit 20
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #11
# MAGIC    select
# MAGIC         ps_partkey,
# MAGIC         round(sum(ps_supplycost * ps_availqty), 2) as value
# MAGIC     from
# MAGIC         partsupp,
# MAGIC         supplier,
# MAGIC         nation
# MAGIC     where
# MAGIC         ps_suppkey = s_suppkey
# MAGIC         and s_nationkey = n_nationkey
# MAGIC         and n_name = 'GERMANY'
# MAGIC     group by
# MAGIC         ps_partkey having
# MAGIC                 sum(ps_supplycost * ps_availqty) > (
# MAGIC             select
# MAGIC                 sum(ps_supplycost * ps_availqty) * 0.0001
# MAGIC             from
# MAGIC                 partsupp,
# MAGIC                 supplier,
# MAGIC                 nation
# MAGIC             where
# MAGIC                 ps_suppkey = s_suppkey
# MAGIC                 and s_nationkey = n_nationkey
# MAGIC                 and n_name = 'GERMANY'
# MAGIC             )
# MAGIC         order by
# MAGIC             value desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #13
# MAGIC    select
# MAGIC         l_shipmode,
# MAGIC         sum(case
# MAGIC             when o_orderpriority = '1-URGENT'
# MAGIC                 or o_orderpriority = '2-HIGH'
# MAGIC                 then 1
# MAGIC             else 0
# MAGIC         end) as high_line_count,
# MAGIC         sum(case
# MAGIC             when o_orderpriority <> '1-URGENT'
# MAGIC                 and o_orderpriority <> '2-HIGH'
# MAGIC                 then 1
# MAGIC             else 0
# MAGIC         end) as low_line_count
# MAGIC     from
# MAGIC         orders,
# MAGIC         lineitem
# MAGIC     where
# MAGIC         o_orderkey = l_orderkey
# MAGIC         and l_shipmode in ('MAIL', 'SHIP')
# MAGIC         and l_commitdate < l_receiptdate
# MAGIC         and l_shipdate < l_commitdate
# MAGIC         and l_receiptdate >= date '1994-01-01'
# MAGIC         and l_receiptdate < date '1994-01-01' + interval '1' year
# MAGIC     group by
# MAGIC         l_shipmode
# MAGIC     order by
# MAGIC         l_shipmode
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #14
# MAGIC     select
# MAGIC         c_count, count(*) as custdist
# MAGIC     from (
# MAGIC         select
# MAGIC             c_custkey,
# MAGIC             count(o_orderkey)
# MAGIC         from
# MAGIC             customer left outer join orders on
# MAGIC             c_custkey = o_custkey
# MAGIC             and o_comment not like '%special%requests%'
# MAGIC         group by
# MAGIC             c_custkey
# MAGIC         )as c_orders (c_custkey, c_count)
# MAGIC     group by
# MAGIC         c_count
# MAGIC     order by
# MAGIC         custdist desc,
# MAGIC         c_count desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #15
# MAGIC    select
# MAGIC         round(100.00 * sum(case
# MAGIC             when p_type like 'PROMO%'
# MAGIC                 then l_extendedprice * (1 - l_discount)
# MAGIC             else 0
# MAGIC         end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
# MAGIC     from
# MAGIC         lineitem,
# MAGIC         part
# MAGIC     where
# MAGIC         l_partkey = p_partkey
# MAGIC         and l_shipdate >= date '1995-09-01'
# MAGIC         and l_shipdate < date '1995-09-01' + interval '1' month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #16
# MAGIC   create or replace temp view revenue (supplier_no, total_revenue) as
# MAGIC         select
# MAGIC             l_suppkey,
# MAGIC             sum(l_extendedprice * (1 - l_discount))
# MAGIC         from
# MAGIC             lineitem
# MAGIC         where
# MAGIC             l_shipdate >= date '1996-01-01'
# MAGIC             and l_shipdate < date '1996-01-01' + interval '3' month
# MAGIC         group by
# MAGIC             l_suppkey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #16
# MAGIC
# MAGIC    select
# MAGIC         s_suppkey,
# MAGIC         s_name,
# MAGIC         s_address,
# MAGIC         s_phone,
# MAGIC         total_revenue
# MAGIC     from
# MAGIC         supplier,
# MAGIC         revenue
# MAGIC     where
# MAGIC         s_suppkey = supplier_no
# MAGIC         and total_revenue = (
# MAGIC             select
# MAGIC                 max(total_revenue)
# MAGIC             from
# MAGIC                 revenue
# MAGIC         )
# MAGIC     order by
# MAGIC         s_suppkey
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #17
# MAGIC    select
# MAGIC         p_brand,
# MAGIC         p_type,
# MAGIC         p_size,
# MAGIC         count(distinct ps_suppkey) as supplier_cnt
# MAGIC     from
# MAGIC         partsupp,
# MAGIC         part
# MAGIC     where
# MAGIC         p_partkey = ps_partkey
# MAGIC         and p_brand <> 'Brand#45'
# MAGIC         and p_type not like 'MEDIUM POLISHED%'
# MAGIC         and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
# MAGIC         and ps_suppkey not in (
# MAGIC             select
# MAGIC                 s_suppkey
# MAGIC             from
# MAGIC                 supplier
# MAGIC             where
# MAGIC                 s_comment like '%Customer%Complaints%'
# MAGIC         )
# MAGIC     group by
# MAGIC         p_brand,
# MAGIC         p_type,
# MAGIC         p_size
# MAGIC     order by
# MAGIC         supplier_cnt desc,
# MAGIC         p_brand,
# MAGIC         p_type,
# MAGIC         p_size
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #18
# MAGIC     select
# MAGIC         c_name,
# MAGIC         c_custkey,
# MAGIC         o_orderkey,
# MAGIC         to_date(o_orderdate) as o_orderdat,
# MAGIC         o_totalprice,
# MAGIC         DOUBLE(sum(l_quantity)) as col6
# MAGIC     from
# MAGIC         customer,
# MAGIC         orders,
# MAGIC         lineitem
# MAGIC     where
# MAGIC         o_orderkey in (
# MAGIC             select
# MAGIC                 l_orderkey
# MAGIC             from
# MAGIC                 lineitem
# MAGIC             group by
# MAGIC                 l_orderkey having
# MAGIC                     sum(l_quantity) > 300
# MAGIC         )
# MAGIC         and c_custkey = o_custkey
# MAGIC         and o_orderkey = l_orderkey
# MAGIC     group by
# MAGIC         c_name,
# MAGIC         c_custkey,
# MAGIC         o_orderkey,
# MAGIC         o_orderdate,
# MAGIC         o_totalprice
# MAGIC     order by
# MAGIC         o_totalprice desc,
# MAGIC         o_orderdate
# MAGIC     limit 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #19
# MAGIC    select
# MAGIC         round(sum(l_extendedprice* (1 - l_discount)), 2) as revenue
# MAGIC     from
# MAGIC         lineitem,
# MAGIC         part
# MAGIC     where
# MAGIC         (
# MAGIC             p_partkey = l_partkey
# MAGIC             and p_brand = 'Brand#12'
# MAGIC             and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
# MAGIC             and l_quantity >= 1 and l_quantity <= 1 + 10
# MAGIC             and p_size between 1 and 5
# MAGIC             and l_shipmode in ('AIR', 'AIR REG')
# MAGIC             and l_shipinstruct = 'DELIVER IN PERSON'
# MAGIC         )
# MAGIC         or
# MAGIC         (
# MAGIC             p_partkey = l_partkey
# MAGIC             and p_brand = 'Brand#23'
# MAGIC             and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
# MAGIC             and l_quantity >= 10 and l_quantity <= 20
# MAGIC             and p_size between 1 and 10
# MAGIC             and l_shipmode in ('AIR', 'AIR REG')
# MAGIC             and l_shipinstruct = 'DELIVER IN PERSON'
# MAGIC         )
# MAGIC         or
# MAGIC         (
# MAGIC             p_partkey = l_partkey
# MAGIC             and p_brand = 'Brand#34'
# MAGIC             and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
# MAGIC             and l_quantity >= 20 and l_quantity <= 30
# MAGIC             and p_size between 1 and 15
# MAGIC             and l_shipmode in ('AIR', 'AIR REG')
# MAGIC             and l_shipinstruct = 'DELIVER IN PERSON'
# MAGIC         )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #20
# MAGIC     select
# MAGIC         s_name,
# MAGIC         s_address
# MAGIC     from
# MAGIC         supplier,
# MAGIC         nation
# MAGIC     where
# MAGIC         s_suppkey in (
# MAGIC             select
# MAGIC                 ps_suppkey
# MAGIC             from
# MAGIC                 partsupp
# MAGIC             where
# MAGIC                 ps_partkey in (
# MAGIC                     select
# MAGIC                         p_partkey
# MAGIC                     from
# MAGIC                         part
# MAGIC                     where
# MAGIC                         p_name like 'forest%'
# MAGIC                 )
# MAGIC                 and ps_availqty > (
# MAGIC                     select
# MAGIC                         0.5 * sum(l_quantity)
# MAGIC                     from
# MAGIC                         lineitem
# MAGIC                     where
# MAGIC                         l_partkey = ps_partkey
# MAGIC                         and l_suppkey = ps_suppkey
# MAGIC                         and l_shipdate >= date '1994-01-01'
# MAGIC                         and l_shipdate < date '1994-01-01' + interval '1' year
# MAGIC                 )
# MAGIC         )
# MAGIC         and s_nationkey = n_nationkey
# MAGIC         and n_name = 'CANADA'
# MAGIC     order by
# MAGIC         s_name
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #21
# MAGIC     select
# MAGIC         s_name,
# MAGIC         count(*) as numwait
# MAGIC     from
# MAGIC         supplier,
# MAGIC         lineitem l1,
# MAGIC         orders,
# MAGIC         nation
# MAGIC     where
# MAGIC         s_suppkey = l1.l_suppkey
# MAGIC         and o_orderkey = l1.l_orderkey
# MAGIC         and o_orderstatus = 'F'
# MAGIC         and l1.l_receiptdate > l1.l_commitdate
# MAGIC         and exists (
# MAGIC             select
# MAGIC                 *
# MAGIC             from
# MAGIC                 lineitem l2
# MAGIC             where
# MAGIC                 l2.l_orderkey = l1.l_orderkey
# MAGIC                 and l2.l_suppkey <> l1.l_suppkey
# MAGIC         )
# MAGIC         and not exists (
# MAGIC             select
# MAGIC                 *
# MAGIC             from
# MAGIC                 lineitem l3
# MAGIC             where
# MAGIC                 l3.l_orderkey = l1.l_orderkey
# MAGIC                 and l3.l_suppkey <> l1.l_suppkey
# MAGIC                 and l3.l_receiptdate > l3.l_commitdate
# MAGIC         )
# MAGIC         and s_nationkey = n_nationkey
# MAGIC         and n_name = 'SAUDI ARABIA'
# MAGIC     group by
# MAGIC         s_name
# MAGIC     order by
# MAGIC         numwait desc,
# MAGIC         s_name
# MAGIC     limit 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #22
# MAGIC     select
# MAGIC         cntrycode,
# MAGIC         count(*) as numcust,
# MAGIC         sum(c_acctbal) as totacctbal
# MAGIC     from (
# MAGIC         select
# MAGIC             substring(c_phone from 1 for 2) as cntrycode,
# MAGIC             c_acctbal
# MAGIC         from
# MAGIC             customer
# MAGIC         where
# MAGIC             substring(c_phone from 1 for 2) in
# MAGIC                 ("13","31","23", "29", "30", "18", "17")
# MAGIC             and c_acctbal > (
# MAGIC                 select
# MAGIC                     avg(c_acctbal)
# MAGIC                 from
# MAGIC                     customer
# MAGIC                 where
# MAGIC                     c_acctbal > 0.00
# MAGIC                     and substring (c_phone from 1 for 2) in
# MAGIC                         ("13","31","23", "29", "30", "18", "17")
# MAGIC             )
# MAGIC             and not exists (
# MAGIC                 select
# MAGIC                     *
# MAGIC                 from
# MAGIC                     orders
# MAGIC                 where
# MAGIC                     o_custkey = c_custkey
# MAGIC             )
# MAGIC         ) as custsale
# MAGIC     group by
# MAGIC         cntrycode
# MAGIC     order by
# MAGIC         cntrycode
# MAGIC

# COMMAND ----------

print(f"Time spend: {time.time() - ts:.2f}")
