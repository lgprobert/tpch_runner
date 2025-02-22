create table supplier (
   s_suppkey integer,
   s_name varchar,
   s_address varchar,
   s_nationkey integer,
   s_phone varchar,
   s_acctbal decimal(15,2),
   s_comment varchar
);

create table part (
   p_partkey integer,
   p_name varchar,
   p_mfgr varchar,
   p_brand varchar,
   p_type varchar,
   p_size integer,
   p_container varchar,
   p_retailprice decimal(15,2),
   p_comment varchar
);

create table partsupp (
   ps_partkey integer,
   ps_suppkey integer,
   ps_availqty integer,
   ps_supplycost decimal(15,2),
   ps_comment varchar
);


CREATE TABLE customer (
    c_custkey INTEGER ,
    c_name VARCHAR,
    c_address VARCHAR,
    c_nationkey INTEGER,
    c_phone VARCHAR,
    c_acctbal DECIMAL(12, 2),
    c_mktsegment VARCHAR,
    c_comment VARCHAR
);

-- Create an orders table
CREATE TABLE orders (
    o_orderkey INTEGER ,
    o_custkey INTEGER,
    o_orderstatus CHAR,
    o_totalprice DECIMAL(12, 2),
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR
);

-- Create a lineitem table
CREATE TABLE lineitem (
    l_orderkey INTEGER,
    l_partkey INTEGER,
    l_suppkey INTEGER,
    l_linenumber INTEGER,
    l_quantity DECIMAL(12, 2),
    l_extendedprice DECIMAL(12, 2),
    l_discount DECIMAL(12, 2),
    l_tax DECIMAL(12, 2),
    l_returnflag CHAR,
    l_linestatus CHAR,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR,
    l_shipmode VARCHAR,
    l_comment VARCHAR
);

create table nation (
   n_nationkey integer,
   n_name varchar,
   n_regionkey integer,
   n_comment varchar
);

create table region (
   r_regionkey integer,
   r_name varchar,
   r_comment varchar
);

