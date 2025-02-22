insert into region
select r_regionkey, r_name, r_comment from (csv::'node://node1/region.tbl')
as r(
    r_regionkey integer,
    r_name varchar(25),
    r_comment varchar(152)
) ;

insert into nation
select n_nationkey, n_name, n_regionkey, n_comment from (csv::'node://node1/nation.tbl')
as n(
    n_nationkey integer,
    n_name varchar(25),
    n_regionkey integer,
    n_comment varchar(152)
) ;

insert into customer
select c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment
from (csv::'node://node1/customer.tbl')
as c(
    c_custkey integer,
    c_name varchar(25),
    c_address varchar(40),
    c_nationkey integer,
    c_phone varchar(15),
    c_acctbal decimal(15,2),
    c_mktsegment varchar(10),
    c_comment varchar(117)
) ;

insert into supplier
select s_suppkey, s_name, s_address,  s_nationkey, s_phone, s_acctbal, s_comment
from (csv::'node://node1/supplier.tbl')
as s(
    s_suppkey integer,
    s_name varchar(25),
    s_address varchar(40),
    s_nationkey integer,
    s_phone varchar(15),
    s_acctbal decimal(15,2),
    s_comment varchar(101)
) ;

insert into part
select p_partkey, p_name, p_mfgr,  p_brand, p_type, p_size, p_container, p_retailprice, p_comment
from (csv::'node://node1/part.tbl')
as s(
    p_partkey integer ,
    p_name varchar(55) ,
    p_mfgr varchar(25) ,
    p_brand varchar(10) ,
    p_type varchar(25) null,
    p_size integer ,
    p_container varchar(10),
    p_retailprice decimal(15,2) ,
    p_comment varchar(23)
) ;

insert into partsupp
select ps_partkey, ps_suppkey, ps_availqty,  ps_supplycost, ps_comment
from (csv::'node://node1/partsupp.tbl')
as s(
    ps_partkey integer ,
    ps_suppkey integer ,
    ps_availqty integer,
    ps_supplycost decimal(15,2),
    ps_comment varchar(199)
) ;

insert into orders
select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
    o_orderpriority, o_clerk, o_shippriority, o_comment
from (csv::'node://node1/orders.tbl')
as s(
    o_orderkey integer ,
    o_custkey integer ,
    o_orderstatus varchar(1) ,
    o_totalprice decimal(15,2) ,
    o_orderdate timestamp ,
    o_orderpriority varchar(15) ,
    o_clerk varchar(15) ,
    o_shippriority integer ,
    o_comment varchar(79)
) ;

insert into lineitem
select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
from (csv::'node://node1/lineitem.tbl')
as s(
    l_orderkey integer ,
    l_partkey integer ,
    l_suppkey integer ,
    l_linenumber integer ,
    l_quantity decimal ,
    l_extendedprice decimal(15,2) ,
    l_discount decimal(15,2) ,
    l_tax decimal(15,2) ,
    l_returnflag varchar(1) ,
    l_linestatus varchar(1) ,
    l_shipdate timestamp ,
    l_commitdate timestamp ,
    l_receiptdate timestamp ,
    l_shipinstruct varchar(25) ,
    l_shipmode varchar(10) ,
    l_comment varchar(44)
) ;
