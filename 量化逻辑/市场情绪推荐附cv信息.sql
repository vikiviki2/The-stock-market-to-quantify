with x1 as( 
select t.`日期`,t.`股票代码`,t.`股票名称`, t.`看涨` from market_s_new t
where t.`看涨`>65),
 x2 as(
SELECT
	x1.*,
	t2.最新价,
	round(if(t1.mean is not null and t2.最新价 is not null , t1.mean - t2.最新价,null),2) 低于平均值,
	t1.cv,
	t1.mean,
	t1.std,
	t3.`流通市值亿`,
	case when t3.`流通市值亿`> 50 then "大盘股"
	     when t3.`流通市值亿`<= 50  and  t3.`流通市值亿`> 5  then "中盘股"
			 when t3.`流通市值亿`<= 5  and  t3.`流通市值亿`> 0  then "小盘股" 
			 else '无' end 股盘,
	t4.`动作`,
	t4.`持股数_万`,
	t4.`数量_万股`,
	t4.`原价` 投资价格,
	t4.`更新日期` 牛散更新日期,
	ROW_NUMBER() OVER (partition BY  x1.`股票代码` order BY t4.`更新日期`  DESC)  AS rn
FROM x1 
  left join stock_hic_label_new  t1 on x1.`股票代码` = t1.`股票代码`
	left join  stock_weekly_detial_new t3 on x1.`股票代码` = t3.`股票代码`
	left join stock_dm_fresh_detail_new t2  ON x1.`股票代码` = upper(t2.`股票代码`)
	and x1.`日期` = t2.`日期`
	left join stock_holder_detail_new t4 ON x1.`股票代码` = t4.`股票代码` 
where t2.最新价<50
and substr(x1.`股票名称`,1,2)<>'退市'
and substr(x1.`股票名称`,1,2)<>'ST'
and t3.`流通市值亿`>= 5
ORDER BY
	看涨 desc ,round(if(t1.mean is not null and t2.最新价 is not null , t1.mean - t2.最新价,null),2) 
	limit 10)
	select *from x2
	where rn =1
	