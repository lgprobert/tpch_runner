-- Q15

with revenue_q15 (supplier_no, total_revenue) as
   (select l_suppkey, sum(l_extendedprice * (1 - l_discount))
      from LINEITEM
     where l_shipdate >= '1996-01-01 00:00:00'
       and l_shipdate < '1996-04-01 00:00:00'
     group by l_suppkey)

    select s_suppkey, s_name, s_address, s_phone, total_revenue
      from SUPPLIER, revenue_q15
     where s_suppkey = supplier_no
       and total_revenue = ( select max(total_revenue)
                               from revenue_q15 )
     order by s_suppkey;
