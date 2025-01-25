create table moxe.SUPPLIER (
   s_suppkey integer not null,
   s_name varchar(25) not null,
   s_address varchar(40) not null,
   s_nationkey integer not null,
   s_phone varchar(15) not null,
   s_acctbal decimal(15,2) not null,
   s_comment varchar(101) not null
) PARTITION (s_suppkey);

create table moxe.PART (
   p_partkey integer not null,
   p_name varchar(55) not null,
   p_mfgr varchar(25) not null,
   p_brand varchar(10) not null,
   p_type varchar(25) null,
   p_size integer not null,
   p_container varchar(10),
   p_retailprice decimal(15,2) not null,
   p_comment varchar(23) not null
) PARTITION (p_partkey);

create table moxe.PARTSUPP (
   ps_partkey integer not null,
   ps_suppkey integer not null,
   ps_availqty integer not null,
   ps_supplycost decimal(15,2) not null,
   ps_comment varchar(199) not null
) PARTITION (ps_partkey);

create table moxe.CUSTOMER (
   c_custkey integer not null,
   c_name varchar(25) not null,
   c_address varchar(40) not null,
   c_nationkey integer not null,
   c_phone varchar(15) not null,
   c_acctbal decimal(15,2) not null,
   c_mktsegment varchar(10),
   c_comment varchar(117)
) PARTITION (c_custkey);

create table moxe.ORDERS (
   o_orderkey integer not null,
   o_custkey integer not null,
   o_orderstatus varchar(1) not null,
   o_totalprice decimal(15,2) not null,
   o_orderdate timestamp not null,
   o_orderpriority varchar(15) not null,
   o_clerk varchar(15) not null,
   o_shippriority integer not null,
   o_comment varchar(79) not null
) PARTITION (o_orderkey);

create table moxe.LINEITEM (
   l_orderkey integer not null,
   l_partkey integer not null,
   l_suppkey integer not null,
   l_linenumber integer not null,
   l_quantity decimal not null,
   l_extendedprice decimal(15,2) not null,
   l_discount decimal(15,2) not null,
   l_tax decimal(15,2) not null,
   l_returnflag varchar(1) not null,
   l_linestatus varchar(1) not null,
   l_shipdate timestamp not null,
   l_commitdate timestamp not null,
   l_receiptdate timestamp not null,
   l_shipinstruct varchar(25) not null,
   l_shipmode varchar(10) not null,
   l_comment varchar(44) not null
) PARTITION (l_orderkey);

create table moxe.NATION (
   n_nationkey integer not null,
   n_name varchar(25) not null,
   n_regionkey integer not null,
   n_comment varchar(152)
);

create table moxe.REGION (
   r_regionkey integer not null,
   r_name varchar(25) not null,
   r_comment varchar(152)
);
