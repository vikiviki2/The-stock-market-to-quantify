with t as (
SELECT distinct
	t.`更新日期`,
	t.`股票名称`,
	t.`股票代码`,
	t.`动作`,
	sum(t.`持股数_万`) 持股数_万,
	sum(t.`数量_万股`) 数量_万股,
	avg(t.`原价` )投资价格
	FROM stock_holder_detail_new t
	where t.`股东名称`='张武'
	GROUP BY 	t.`更新日期`,
	t.`股票名称`,
	t.`股票代码`,
	t.`动作`
)
select a.* from (
SELECT distinct
    t.*,
	t1.最新价 ,
	round(t.投资价格-t1.最新价,3)  低于其投资价格,
	round(t2.mean-t1.最新价,3)  低于平均价格,
	round(t2.cv,3) cv,
	round(t2.mean,3) mean,
	round(t2.std,3) std,
	CASE
	WHEN t3.`流通市值亿` > 700 THEN
	"大盘股大于700" 
	WHEN t3.`流通市值亿` <= 700 AND t3.`流通市值亿` > 400 THEN
	"中盘股400至700" 
	WHEN t3.`流通市值亿` <= 400 AND t3.`流通市值亿` > 200 THEN
	"小盘股200至400" 
	WHEN t3.`流通市值亿` <= 200 AND t3.`流通市值亿` > 150 THEN
	"较小盘股150至200" 
	ELSE '无' 
	END 股盘 ,
	t3.`流通市值亿`,
	t4.`看涨`,
	t4.`日期` 市场情绪日期	,
	ROW_NUMBER() OVER (partition BY  t4.`股票代码`, t.更新日期 order BY t4.`日期`  DESC)  AS rn
FROM t
	LEFT JOIN stock_dm_fresh_detail_new t1 ON t.`股票代码` = t1.`股票代码`
	LEFT JOIN stock_hic_label_new t2 ON t1.`股票代码` = t2.`股票代码` 
	LEFT JOIN stock_weekly_detial_new t3 ON t.`股票代码` = t3.`股票代码` 
	left join market_s t4 ON t.`股票代码` = t4.`股票代码` 
WHERE
	t.动作 like '%增加%' 
	AND substr( t.`股票名称`, 1, 2 ) <> '退市' 
	AND substr( t.`股票名称`, 1, 2 ) <> 'ST' 
	AND t1.最新价 - t.`投资价格` < 0 
	AND t1.最新价 - t.`投资价格` >- 10 
  and t.`更新日期` >  SUBDATE(curdate(),180 )	
	and t3.`流通市值亿`>100
	#and cv<0.15
	and t1.最新价 <30
	and t.`股票代码` not in (select DISTINCT 股票代码  from stock_holder_detail t
where 动作  like '%减少%'  
and `更新日期` >  SUBDATE(curdate(),30 ))
ORDER BY
 t.`更新日期` desc,
t1.最新价 - t.`投资价格` ASC
)a
where a.rn =1