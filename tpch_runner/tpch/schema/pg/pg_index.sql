CREATE INDEX IF NOT EXISTS IDX_SUPPLIER_NATION_KEY ON SUPPLIER (S_NATIONKEY);

CREATE INDEX IF NOT EXISTS IDX_PARTSUPP_PARTKEY ON PARTSUPP (PS_PARTKEY);
CREATE INDEX IF NOT EXISTS IDX_PARTSUPP_SUPPKEY ON PARTSUPP (PS_SUPPKEY);

CREATE INDEX IF NOT EXISTS IDX_CUSTOMER_NATIONKEY ON CUSTOMER (C_NATIONKEY);

CREATE INDEX IF NOT EXISTS IDX_ORDERS_CUSTKEY ON ORDERS (O_CUSTKEY);

CREATE INDEX IF NOT EXISTS IDX_LINEITEM_ORDERKEY ON LINEITEM (L_ORDERKEY);
CREATE INDEX IF NOT EXISTS IDX_LINEITEM_PART_SUPP ON LINEITEM (L_PARTKEY,L_SUPPKEY);

CREATE INDEX IF NOT EXISTS IDX_NATION_REGIONKEY ON NATION (N_REGIONKEY);

CREATE INDEX IF NOT EXISTS IDX_LINEITEM_SHIPDATE ON LINEITEM (L_SHIPDATE, L_DISCOUNT, L_QUANTITY);

CREATE INDEX IF NOT EXISTS IDX_ORDERS_ORDERDATE ON ORDERS (O_ORDERDATE);
