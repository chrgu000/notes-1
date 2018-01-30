1.源文件
sftp
地址:10.30.2.235
端口:22
账号:GetFile
密码:GetFile#123
目录:/usrcol1/unicom/uniload/data/DAL/DailyTraffic/binyang-jz
文件名:DUANXIN-1220&BY-MO.zip(宾阳)
	   DUANXIN-1220&NN-MO.zip(南宁)
压缩内容: 第一个：DUANXIN-1220.csv
		  其余：DUANXIN-1220-x.csv(x从2开始)

2.处理方法:
1)采集数据生成总表
create table HJX_20171213_SMS
(
  start_time     VARCHAR2(256)--开始时间,
  end_time       VARCHAR2(256)--结束时间,
  device_number  VARCHAR2(256)--用户号码,
  imsi           VARCHAR2(256)--用户IMSI,
  opp_number     VARCHAR2(256)--对端号码,
  opp_imsi       VARCHAR2(256)--对端IMSI,
  imei           VARCHAR2(256)--IMEI/IMEISV,
  model_type     VARCHAR2(256)--终端类型,
  sms_type       VARCHAR2(256)--短信类型,
  pro_result     VARCHAR2(256)--过程结果,
  fail_reason    VARCHAR2(256)--失败原因,
  release_reason VARCHAR2(256)--释放原因,
  innet_type     VARCHAR2(256)--接入网类型,
  city_name      VARCHAR2(256)--地市,
  msc            VARCHAR2(256)--MSC,
  bsc            VARCHAR2(256)--BSC/RNC,
  hlr            VARCHAR2(256)--HLR,
  smc            VARCHAR2(256)--SMC,
  lai            VARCHAR2(256)--LAI,
  cgi            VARCHAR2(256)--CGI/SAI,
  sms_number     VARCHAR2(256)--短信中心号码
)

2)生成4个目标表
--省外
select nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他') --省份,
trim(substr(c.comments, instr(c.comments, '.', 1, 1) +1)) --地市,
a.device_number,count(*) --号码,
count(case when pro_result='"成功"' then 1 end) --发送记录数,
--count(case when pro_result='"成功"' then 1 end)/count(*) --发送成功数,
b.imei --imei,
count(distinct cgi) --cgi
from HJX_20171213_SMS a, --
 ( select * from 
   ( select t.*,row_number()over(partition by device_number order by cnt desc) rn  
     from (
           select device_number, imei, count(*) cnt
             from HJX_20171213_SMS  --
            --where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
            group by device_number, imei
          ) t
   ) where rn=1 ) b ,
   hjx_mobile_prefix c 
 where a.device_number=b.device_number(+) 
 and a.device_number<>'""'
 --and substr(replace(a.end_time,'-',''),2,8)='20171213' --
 and substr(a.device_number, 2, 7) = c.mobile_prefix(+)
 and nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他') not in ('广西','南宁','桂林','柳州','来宾','玉林','贵港','崇左','百色','北海','钦州','河池','梧州','贺州')
 and a.device_number not like '"170%'
 and a.device_number not like '"171%'
 group by nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他'),
trim(substr(c.comments, instr(c.comments, '.', 1, 1) +1)),a.device_number ,b.imei 
order by 4 desc;

--广西
select nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他') --省份,
trim(substr(c.comments, instr(c.comments, '.', 1, 1) +1)) --地市,
a.device_number,count(*) --号码,
count(case when pro_result='"成功"' then 1 end) --发送记录数,
--count(case when pro_result='"成功"' then 1 end)/count(*) --发送成功数,
b.imei --imei,
count(distinct cgi) --cgi
from HJX_20171213_SMS a, --
 ( select * from 
   ( select t.*,row_number()over(partition by device_number order by cnt desc) rn  
     from (
           select device_number, imei, count(*) cnt
             from HJX_20171213_SMS  --
            --where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
            group by device_number, imei
          ) t
   ) where rn=1 ) b ,
   hjx_mobile_prefix c 
 where a.device_number=b.device_number(+) 
 and a.device_number<>'""'
 --and substr(replace(a.end_time,'-',''),2,8)='20171213' --
 and substr(a.device_number, 2, 7) = c.mobile_prefix(+)
 and nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他') in ('广西','南宁','桂林','柳州','来宾','玉林','贵港','崇左','百色','北海','钦州','河池','梧州','贺州')
 and a.device_number not like '"170%'
 and a.device_number not like '"171%'
 group by nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他'),
trim(substr(c.comments, instr(c.comments, '.', 1, 1) +1)),a.device_number ,b.imei 
order by 4 desc;

--虚商
select  
a.device_number --号码,count(*) --发送记录数,
count(case when pro_result='"成功"' then 1 end) --发送成功数,
--count(case when pro_result='"成功"' then 1 end)/count(*),
b.imei --imei,
count(distinct cgi) --cgi
from HJX_20171213_SMS a, --
 ( select * from 
   ( select t.*,row_number()over(partition by device_number order by cnt desc) rn  
     from (
           select device_number, imei, count(*) cnt
             from HJX_20171213_SMS  --
            --where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
            group by device_number, imei
          ) t
   ) where rn=1 ) b ,
   hjx_mobile_prefix c 
 where a.device_number=b.device_number(+) 
 and a.device_number<>'""'
 --and substr(replace(a.end_time,'-',''),2,8)='20171213' --
 and substr(a.device_number, 2, 7) = c.mobile_prefix(+)
 --and nvl(substr(c.comments,1,instr(c.comments,'.',1,1)-1),'其他')<>'广西'
 and (a.device_number  like '"170%'
 or a.device_number  like '"171%')
 group by  a.device_number ,b.imei 
order by 2 desc;

--CGI
select cgi --cgi,
count(*) --记录数,
imei--imei
from HJX_20171213_SMS --
where substr(replace(end_time, '-', ''), 2, 8) = '20171213' --
group by cgi,imei order by 2 desc; 

3.目标表
注意南宁和宾阳分开
虚商(号码	发送记录数	发送成功数	IMEI（使用最多的一个）	基站数(CGI))
省内(地市	号码	发送记录数	发送成功数	IMEI（使用最多的一个）	基站数（CGI）)
省外(省份	地市	号码	发送记录数	发送成功数	IMEI（使用最多的一个）	基站数（CGI）)
CGI(CGI	发送记录数	IMEI)
