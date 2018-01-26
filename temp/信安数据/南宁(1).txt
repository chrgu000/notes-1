前面处理过程同宾阳

目标表：
1.imei
--IMEI
select imei --imei,
count(*) --发送记录数,
count(case when pro_result='"成功"' then 1 end) --发送成功数,
--count(case when pro_result='"成功"' then 1 end)/count(*),
count(distinct device_number) --号码数,
count(distinct cgi) --cgi
from zhaizh_nanning_20171213_SMS  --表名需修改
where substr(replace(end_time,'-',''),2,8)='20171213'  --日期需修改
group by imei order by 2 desc; 


2.号码
--按号码（带归属地）
select a.device_number --号码,
count(*) --发送记录数,
count(case when pro_result='"成功"' then 1 end) --发送成功数,
--count(case when pro_result='"成功"' then 1 end)/count(*),
b.imei --imei,
count(distinct cgi) --cgi,
max(province_name) province_name --归属地
from zhaizh_nanning_20171213_SMS a, --
 ( select * from 
   ( select t.*,row_number()over(partition by device_number order by cnt desc) rn  
     from (
           select device_number, imei, count(*) cnt
             from zhaizh_nanning_20171213_SMS  --
            where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
            group by device_number, imei
          ) t
   ) where rn=1 ) b,
(
  select t.device_number,
         substr(t1.comments, 1, instr(t1.comments, '.', 1, 1) - 1) province_name 
    from (select distinct device_number
            from zhaizh_nanning_20171213_SMS --
           where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
          ) t,
         hjx_mobile_prefix t1
   where substr(t.device_number, 2, 7) = t1.mobile_prefix(+)
) c
 where a.device_number=b.device_number(+) 
 and substr(replace(a.end_time,'-',''),2,8)='20171213' --
 and a.device_number=c.device_number(+)
 group by a.device_number ,b.imei 
 order by 2 desc; 

3.cgi
--按CGI
select cgi,count(*),imei from zhaizh_nanning_20171213_SMS --
where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
group by cgi,imei order by 2 desc ;