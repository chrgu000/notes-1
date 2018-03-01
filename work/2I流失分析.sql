select
sum(total_flux) as total_flux,	--总流量
sum(bill_flux) as bill_flux,	--套外流量
sum(FLUX_3G) as FLUX_3G,	--3G流量
sum(FLUX_4G) as FLUX_4G	--4G流量
FROM
DWA.DWA_V_D_CUS_CB_SING_FLUX
WHERE MONTH_ID = '201802'
AND DAY_ID < '26'
AND DEVICE_NUMBER = '18587586751';


select * from lt_huangjunxin.temp_20180226_stopuser;

select 
b.MONTH_ID,b.DEVICE_NUMBER,a.user_id,
b.total_flux,	--总流量
b.bill_flux,	--套外流量
b.FLUX_3G,	--3G流量
b.FLUX_4G,	--4G流量
c.CALL_DURATION
from
(select distinct user_id from lt_huangjunxin.temp_20180226_stopuser) a
left outer join
(select 
month_id,DEVICE_NUMBER,user_id,
sum(total_flux) as total_flux,	--总流量
sum(bill_flux) as bill_flux,	--套外流量
sum(FLUX_3G) as FLUX_3G,	--3G流量
sum(FLUX_4G) as FLUX_4G	--4G流量
from DWA.DWA_V_D_CUS_CB_SING_FLUX 
WHERE MONTH_ID = '201801'
group by month_id,DEVICE_NUMBER,user_id) b
on
a.user_id = b.user_id
left outer join
(select month_id,DEVICE_NUMBER,user_id,sum(CEILING(CALL_DURATION / 60)) as CALL_DURATION from dwa.DWA_S_D_USE_CB_VOICE
WHERE MONTH_ID = '201801'
group by month_id,DEVICE_NUMBER,user_id) c
on
a.user_id = c.user_id;



select
*
from
(select * from lt_huangjunxin.temp_20180226_stopuser limit 10) a
join
DWA.DWA_V_D_CUS_CB_SING_FLUX b
on
a.user_id = b.user_id
WHERE
b.MONTH_ID = '201802'
or b.MONTH_ID = '201801';


select 
*
from
(select * from lt_huangjunxin.temp_20180226_stopuser limit 10) a
join
(select
MONTH_ID,DEVICE_NUMBER,user_id,
sum(total_flux) as total_flux,	--总流量
sum(bill_flux) as bill_flux,	--套外流量
sum(total_fee) as total_fee,
sum(FLUX_3G) as FLUX_3G,	--3G流量
sum(FLUX_4G) as FLUX_4G	--4G流量
from
DWA.DWA_V_D_CUS_CB_SING_FLUX
group by
MONTH_ID,DEVICE_NUMBER,user_id) b
on
a.user_id = b.user_id;



select
sum(total_flux) as total_flux,	--总流量
sum(bill_flux) as bill_flux,	--套外流量
sum(FLUX_3G) as FLUX_3G,	--3G流量
sum(FLUX_4G) as FLUX_4G	--4G流量
from
DWA.DWA_V_D_CUS_CB_SING_FLUX
WHERE MONTH_ID = '201802'
AND DEVICE_NUMBER = '18587586751'
group by
MONTH_ID,DEVICE_NUMBER;



select
*
FROM
DWA.DWA_V_D_CUS_CB_SING_FLUX
WHERE MONTH_ID = '201802'
AND DAY_ID < '26'
AND DEVICE_NUMBER = '18587586751';
