drop table X1;
CREATE TEMPORARY TABLE X1 SELECT
t.`股票名称`,
t.`股票代码`,
t.`更新日期`,
t2.今开,
ROUND( t.mean - t2.今开, 2 ) 低于平均值,
t.cv,
t.mean,
t.std,
t3.`流通市值亿`,
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
	END 股盘 
FROM
	stock_hic_label_new t
	LEFT JOIN stock_dm_fresh_detail_new t2 ON t.`股票代码` = t2.`股票代码`
	LEFT JOIN stock_weekly_detial_new t3 ON t.`股票代码` = t3.`股票代码` 
WHERE
	t3.`流通市值亿` > 50 
ORDER BY
	t.mean - t2.今开 DESC,
	t.cv;
	
drop table X2;
CREATE TEMPORARY TABLE X2 SELECT
* 
FROM
	X1 
WHERE
	`流通市值亿` > 100 
	AND `今开` < 20 
	and cv<0.08
ORDER BY
	cv;
	
select *from (	
SELECT
	x2.*,
	t.`动作`,
	t.`持股数_万`,
	t.`数量_万股`,
	t.`原价` 投资价格,
	t.`更新日期` 牛散更新日期,
	ROW_NUMBER() OVER (partition BY  x2.`股票代码` order BY t.`更新日期`  DESC)  AS rn
FROM
	x2
LEFT JOIN 
stock_holder_detail_new t ON x2.`股票代码` = t.`股票代码` 
order by cv, 低于平均值 desc
)a
where rn=1
and 动作 not like '%减少%'  
order by 低于平均值  desc , 牛散更新日期 