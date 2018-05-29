v_date=$1
if [ -n "$v_date" ]; then echo ""; else v_date=$(date -d '1 day ago' +%Y%m%d) ;fi
  echo "v_date:$v_date"
  v_month_id=`echo $v_date | cut -c 1-6`
  echo "v_month_id:$v_month_id"
  v_day_id=`echo $v_date | cut -c 7-8`
  echo "v_day_id:$v_day_id"
  v_lmonth_1=$(date -d "${v_month_id}01 -1 month" +%Y%m)
  echo "v_lmonth_1:$v_lmonth_1"
  v_lmonth_2=$(date -d "${v_month_id}01 2 month ago" +%Y%m)
  echo "v_lmonth_2:$v_lmonth_2"
  v_lmonth_3=$(date -d "${v_month_id}01 3 month ago" +%Y%m)
  echo "v_lmonth_3:$v_lmonth_3"
  v_lmonth_4=$(date -d "${v_month_id}01 4 month ago" +%Y%m)
  echo "v_lmonth_4:$v_lmonth_4"
  v_lmonth_5=$(date -d "${v_month_id}01 5 month ago" +%Y%m)
  echo "v_lmonth_5:$v_lmonth_5"
  v_lmonth_6=$(date -d "${v_month_id}01 6 month ago" +%Y%m)
  echo "v_lmonth_6:$v_lmonth_6"
  v_month_2=`echo $v_date | cut -c 5-6`
  echo "v_month_2:$v_month_2"

  v_user=lt_mengguanzhou
  v_logfile=manruzhongdiandiqu_maoming.log

#定义sql字符串

v_sql1="
--目标用户
--20171128 add 福建 20180404 add 茂名
insert overwrite table temp_roam_xx_LIST 
SELECT t.USER_ID,
t.device_number,
MAX(SUBSTR(T.IMEI,1,14)) IMEI,
t1.PROVINCE_NAME,
concat(t1.province_code, t1.local_net) as province_code,
min(concat(t.month_id,t.day_id)) start_date,
max(concat(t.month_id,t.day_id)) end_date,
count(distinct t.day_id) stay_days 
FROM DWD.DWD_D_USE_CB_VOICE T 
inner join ( SELECT *
FROM DIM.DIM_PUB_CODE_AREA_CODE T ) t1
on T.MONTH_ID = '$v_month_id' 
and t.day_id<='$v_day_id'-----------------
and t.VISIT_AREA_CODE=t1.area_code         
GROUP BY t.USER_ID,t.device_number,t1.PROVINCE_NAME,t1.province_code, t1.local_net--,a.IMEI
;

insert into table temp_roam_xx_LIST 
select a.user_id,a.device_number,case when b.imei is not null then b.imei else a.imei end ,a.province_name,
start_date,
end_date,
stay_days,
province_code
from (
SELECT t.USER_ID,
t.device_number,
MAX(SUBSTR(T.IMEI,1,14)) IMEI,
t1.PROVINCE_NAME,
concat(t1.province_code, t1.local_net) as province_code,
min(concat(t.month_id,t.day_id)) start_date,
max(concat(t.month_id,t.day_id)) end_date,
count(distinct t.day_id) stay_days 
FROM DWD.DWD_D_USE_mB_VOICE T 
inner join ( SELECT *
FROM DIM.DIM_PUB_CODE_AREA_CODE T ) t1
on T.MONTH_ID = '$v_month_id'
and t.day_id<='$v_day_id' --------------------
and t.CALL_AREA_ID=t1.area_code      
GROUP BY t.USER_ID,t.device_number,t1.PROVINCE_NAME,t1.province_code, t1.local_net--,t.IMEI
) a left join (
--ocs 
select self_number,MAX(SUBSTR(card_no,1,14)) imei
from dwd.DWD_D_USE_MB_CDR_GSM_STAT_RNET t
inner join ( SELECT *
FROM DIM.DIM_PUB_CODE_AREA_CODE T) t1 
on t.visit_area_code=t1.area_code  
and T.MONTH_ID = '$v_month_id'
and t.day_id<='$v_day_id'--------------------
group by self_number     
) b on a.device_number=b.self_number    
;


insert overwrite table HY_BY_LAC_ID 
SELECT T.USER_ID,
COUNT(BY_LAC_ID) BY_LAC_NUM,
COUNT(NOT_BY_LAC_ID) NOT_BY_LAC_NUM
FROM (SELECT T.USER_ID,
CONCAT(T1.LAC_ID, T1.CELL_ID) BY_LAC_ID,
CONCAT(T.LAC_ID, T.CELL_ID) NOT_BY_LAC_ID
FROM DWA.DWA_S_D_USE_MB_BS T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
LEFT JOIN (SELECT T.LAC_ID, T.CELL_ID
FROM DWD.DWD_D_RES_AL_BS_INFO T
WHERE T.MONTH_ID = '$v_month_id'
AND T.DAY_ID = '$v_day_id'
AND (T.CELL_NAME LIKE '%宾阳%' OR T.CITY LIKE '%宾阳%' OR
T.PROFIT_UNIT LIKE '%宾阳%')
GROUP BY T.LAC_ID, T.CELL_ID) T1
ON T.LAC_ID = T1.LAC_ID
AND T.CELL_ID = T1.CELL_ID
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID,
CONCAT(T1.LAC_ID, T1.CELL_ID),
CONCAT(T.LAC_ID, T.CELL_ID)) T
GROUP BY T.USER_ID;

insert overwrite table MID_ROAM_QQ_SUM  
SELECT X.DEVICE_NUMBER, X.QQ_CNT
FROM (SELECT X.DEVICE_NUMBER, COUNT(1) QQ_CNT
FROM (SELECT T.DEVICE_NUMBER, T.URL QQ
FROM OF_DWA.DWA_D_MB_USRQQ T 
INNER JOIN temp_roam_xx_list  X ON T.DEVICE_NUMBER=X.DEVICE_NUMBER
WHERE SUBSTR(T.DAY_ID, 1, 6) = '$v_month_id'
AND LENGTH(T.DEVICE_NUMBER) = 11
AND LENGTH(T.URL) > 5
AND T.SERVICE_TYPE = '301') X
GROUP BY X.DEVICE_NUMBER) X;

insert overwrite table  MID_ROAM_PAY
SELECT T.USER_ID, SUM(T.PAY_CHARGE) PAY_CHARGE
FROM DWD.DWD_D_ACC_AL_PAY T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
GROUP BY T.USER_ID;


insert overwrite table mid_roam_backuser 
SELECT T.USER_ID,
T.BACK_DEPT_DESC,
t.back_dept_no,
T.BACK_CHNL_TYPE,
T.BACK_OPER_NAME,
T.BACK_OPER_DATE
FROM (SELECT T.USER_ID,
T1.CHANNEL_NAME BACK_DEPT_DESC,
t1.channel_no back_dept_no,
T2.CHANNEL_NAME BACK_CHNL_TYPE,
T3.OPER_NAME BACK_OPER_NAME,
T.OPER_DATE BACK_OPER_DATE,
ROW_NUMBER() OVER(PARTITION BY T.USER_ID ORDER BY T.OPER_DATE DESC) RNK_ID
FROM DWD.DWD_D_EVT_AL_LOG_COMM_OCS T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
LEFT JOIN DIM.DIM_PUB_AREA_NEW_ALL T1
ON T.OPER_DEPT_NO = T1.CHANNEL_NO
LEFT JOIN DIM.DIM_SIX_CHANNEL T2
ON T.OPER_DEPT_NO = T2.CHANNEL_NO
LEFT JOIN DIM.DIM_PUB_OPER_NO T3
ON T.OPER_NO = T3.OPER_NO
WHERE T.MONTH_ID = '$v_month_id'
AND T.DAY_ID = '$v_day_id' --20170212之后才有数据
AND T.INST_CODE = 'AU004'
AND T.MEMO = 1) T
WHERE T.RNK_ID = 1;


insert overwrite table hy_voice_numbers_imei 
SELECT a.USER_ID,
'' IMEI,
SUM(CASE WHEN a.CALL_TYPE = '01' THEN a.CDR_NUMS ELSE 0 END) ZJ_TIMES,
ROUND(SUM(CASE WHEN a.CALL_TYPE = '01' THEN a.CALL_DURATION ELSE 0 END) / 60) ZJ_DURA,
sum(case when a.Roam_Type in ('01AA', '0201') then a.CDR_NUMS end) qn_CDR_NUMS, --区内话单条数
sum(case when a.Roam_Type not in ('01AA', '0201') then a.CDR_NUMS end) qw_CDR_NUMS, --区外话单条数
count(distinct case when a.Roam_Type in ('01AA', '0201') then concat(a.LAC_ID, a.CELL_ID) end) qn_LAC_NUMS, --区内基站数量（剔重）
count(distinct case when a.Roam_Type not in ('01AA', '0201') then concat(a.LAC_ID, a.CELL_ID) end) qw_LAC_NUMS --区外基站数量（剔重）
FROM DWA.DWA_S_D_USE_MB_VOICE a INNER JOIN temp_roam_xx_list X ON a.USER_ID=X.USER_ID
WHERE a.MONTH_ID = '$v_month_id'
--AND a.IMEI IS NOT NULL
GROUP BY a.USER_ID;


insert into table hy_voice_numbers_imei 
SELECT a.USER_ID,
'' IMEI,
SUM(CASE WHEN a.CALL_TYPE = '01' THEN 1 ELSE 0 END) ZJ_TIMES,
ROUND(SUM(CASE WHEN a.CALL_TYPE = '01' THEN a.CALL_DURATION ELSE 0 END) / 60) ZJ_DURA,
count(case when a.Roam_Type in ('01AA', '0201') then 1 end) qn_CDR_NUMS, --区内话单条数
count(case when a.Roam_Type not in ('01AA', '0201') then 1 end) qw_CDR_NUMS, --区外话单条数
count(distinct case when a.Roam_Type in ('01AA', '0201') then concat(a.END_LAC_ID, a.END_CELL_ID) end) qn_LAC_NUMS, --区内基站数量（剔重）
count(distinct case when a.Roam_Type not in ('01AA', '0201') then concat(a.END_LAC_ID, a.END_CELL_ID) end) qw_LAC_NUMS        
FROM DWd.DWD_D_USE_CB_VOICE a INNER JOIN temp_roam_xx_list X ON a.USER_ID=X.USER_ID
WHERE a.MONTH_ID = '$v_month_id'
--AND a.IMEI IS NOT NULL
GROUP BY a.USER_ID;


insert overwrite table hy_voice_jf_times_imei
SELECT T.USER_ID, T.OUT_CNT, T.IN_CNT, T.OUT_JF_TIMES, T.IN_JF_TIMES
FROM (SELECT T.USER_ID,
SUM(T.OUT_CNT) OUT_CNT,
SUM(T.IN_CNT) IN_CNT,
SUM(T.OUT_JF_TIMES) OUT_JF_TIMES,
SUM(T.IN_JF_TIMES) IN_JF_TIMES
FROM DWA.DWA_V_D_CUS_MB_SING_VOICE T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) T;

insert into table hy_voice_jf_times_imei
SELECT T.USER_ID, T.OUT_CNT, T.IN_CNT, T.OUT_JF_TIMES, T.IN_JF_TIMES
FROM (SELECT T.USER_ID,
SUM(T.OUT_CNT) OUT_CNT,
SUM(T.IN_CNT) IN_CNT,
SUM(T.OUT_JF_TIMES) OUT_JF_TIMES,
SUM(T.IN_JF_TIMES) IN_JF_TIMES
FROM DWA.DWA_V_D_CUS_CB_SING_VOICE T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) T;


insert overwrite table hy_flux_imei
SELECT T.USER_ID,
'' IMEI,
ROUND(SUM(T.TOTAL_BYTES) / 1024 / 1024, 2) TOTAL_FLUX
FROM DWA.DWA_S_D_USE_MB_FLUX T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
WHERE T.MONTH_ID = '$v_month_id'
-- AND T.IMEI IS NOT NULL
GROUP BY T.USER_ID;

insert into table hy_flux_imei
SELECT T.USER_ID,
'' IMEI,
ROUND(SUM(T.UP_FLUX + T.DOWN_FLUX), 2) TOTAL_FLUX
FROM DWA.DWA_S_D_USE_CB_FLUX T INNER JOIN temp_roam_xx_list X ON T.USER_ID=X.USER_ID
WHERE T.MONTH_ID = '$v_month_id'
--AND T.IMEI IS NOT NULL
GROUP BY T.USER_ID;

--bss
insert overwrite table TP_LH_0626_ROAM_LIST_2 
SELECT B.DEVICE_NUMBER,  --  DEVICE_NUMBER
b1.COMMENTS product_name,
b.product_id,
A.PROVINCE_NAME,  --  PROVINCE_NAME（请单独增加宾阳）
a.province_code,  -- province_code
a.start_date,
a.end_date,
a.stay_days,
C.CITY_NAME,  --  CITY_NAME
c.city_no,  -- city_no
C.AREA_NAME,  --  AREA_NAME
c.area_no,  -- area_no
u.prof_name,
CASE WHEN E.CERT_TYPE_PROV IS NOT NULL THEN '1' ELSE '0' END IS_JK, --  IS_JK
D.REALNAME_FLAG,  --  REALNAME_FLAG
F.USER_STATUS_PROV_DESC,  --  用户状态
F.user_status,  -- 用户状态编码
G.PAY_MODE_DESC,  --  付费类型
g.pay_mode, -- 付费类型
D.CUST_NAME,  --  CUST_NAME
D.PSPT_ID,  --  PSPT_ID
D.ID_ADDRESS, --  ID_ADDRESS
E1.CERT_TYPE_PROV_DESC, --  证件类型
e1.cert_type, -- 证件类型
B.OPER_DEPT_NO, --  OPER_DEPT_NO
C.CHANNEL_NAME, --  CHANNEL_NAME
b.channel_no, -- channel_no
DATE_TOSTRING(B.INNET_DATE, 'YYYYMMDD') INNET_DATE, --  INNET_DATE
H.CHANNEL_TYPE_DESC CHANNEL_TYPE,  --  发展渠道类型
h.chnl_type channel_type_no,  -- 发展渠道类型
s.back_dept_desc, --返档渠道名称
s.back_dept_no, -- 返档渠道
case when k.pay_charge > '0' then '1' else  '0' end is_pay ,--  入网后是否有缴费记录
A.IMEI, --  IMEI
'' imei_if_hekui, --  IMEI是否规范
''  imei_if_zb,--  IMEI是否为涉嫌诈骗终端
A.IF_ROAM,
A.IF_LOCAL,
A.IF_FOCUS,
A.IF_NORMAL,
A.USER_ID,
a.flag_type 
FROM (
SELECT B.USER_ID,
B.IMEI IMEI,
NULL IF_ROAM,
NULL IF_LOCAL,
NULL IF_FOCUS,
NULL IF_NORMAL,
B.PROVINCE_NAME,
b.province_code,
'1' FLAG_TYPE,
start_date,
end_date,
stay_days 
FROM temp_roam_xx_list B
)A
INNER JOIN (SELECT *
FROM DWA.DWA_V_d_CUS_MB_USER_DERIVE T
WHERE T.MONTH_ID = '$v_month_id'
and t.day_id='$v_day_id'
AND T.SERVICE_TYPE IN ('20AAAAAA', '30AAAAAA')) B
ON A.USER_ID = B.USER_ID
left join dim.dim_pub_dinner_service b1
on b.PRODUCT_ID=b1.SERVICE_DINNER
LEFT JOIN (SELECT T.CHANNEL_NO,
MAX(T.CITY_NAME) CITY_NAME,
max(t.city_no) city_no,
MAX(T.AREA_NAME) AREA_NAME,
max(t.area_no) area_no,
MAX(T.CHANNEL_NAME) CHANNEL_NAME,
max(CHANNEL_TYPE_ZB) CHANNEL_TYPE_ZB
FROM DWD.DWD_D_PUB_AL_CHANNEL T
WHERE T.MONTH_ID = '$v_month_id'
AND T.DAY_ID = '$v_day_id'
GROUP BY T.CHANNEL_NO) C
ON B.CHANNEL_NO = C.CHANNEL_NO
INNER JOIN (SELECT CUST_ID,
CUST_NAME,
PSPT_ID_TYPE,
PSPT_ID,
ID_ADDRESS,
LINK_ADDR,
CASE
WHEN REALNAME_TYPE = '103' THEN
'二代+公安'
WHEN REALNAME_TYPE = '102' THEN
'实名-二代'
WHEN REALNAME_TYPE = '101' THEN
'实名-公安'
WHEN REALNAME_TYPE = '100' THEN
'实名-系统'
ELSE
'未实名'
END REALNAME_FLAG
FROM DWD.DWD_D_CUS_AL_CUSTOMER
WHERE MONTH_ID = '$v_month_id'
AND DAY_ID = '$v_day_id') D
ON B.CUST_ID = D.CUST_ID
LEFT JOIN (SELECT CERT_TYPE_PROV
FROM DIM.DIM_REF_CERT_TYPE
WHERE CERT_TYPE_PROV IN ('ID027',
'ID028',
'ID029',
'ID030',
'ID031',
'ID016',
'ID017',
'ID018',
'ID116',
'ID117',
'ID118',
'ID120',
'ID007',
'ID034',
'ID035')) E
ON D.PSPT_ID_TYPE = E.CERT_TYPE_PROV
left join  DIM.DIM_REF_CERT_TYPE E1
ON D.PSPT_ID_TYPE = E1.CERT_TYPE_PROV
left join DIM.DIM_REF_USER_STATUS F
ON b.user_status_prov_up = F.USER_STATUS_PROV
left join DIM.DIM_PUB_PAY_MODE G
ON b.PAY_MODE = G.PAY_MODE
left join (SELECT P.CHANNEL_NO, Q.CHANNEL_TYPE_DESC, q.chnl_type
FROM DIM.DIM_PUB_ZB_AGENT_CODE P
LEFT JOIN DIM.DIM_SIX_CHANNEL Q
ON P.CHNL_CODE = Q.CHANNEL_NO) H
on C.CHANNEL_NO = H.CHANNEL_NO
left join mid_roam_pay k --缴费记录
on a.user_id = k.user_id
left join mid_roam_backuser s --返档渠道
on a.user_id=s.user_id
left join dim.DIM_PUB_CODE_ORG_CHNL u on b.chnl_no=u.chnl_no;


insert overwrite table roam_user_list_maoming partition (month_id = '$v_month_id',day_id='$v_day_id')
SELECT A.*,
B.ZJ_TIMES, --  ZJ_TIMES
B.ZJ_DURA, --  ZJ_DURA
C.DIRE_SMS, --  DIRE_SMS
-- case
-- when h.user_id is not null then
--  '是'
-- else
-- '否'
-- end is_transfer_call, --  是否有呼叫转移话单
-- h.OTHER_NUMBER, --  呼叫转移被叫号码
D.TOTAL_FLUX, --  LOCAL_NET
E.OUT_JF_TIMES, --  OUT_JF_TIMES
E.OUT_CNT, --  主叫次数OUT_CNT
E.IN_JF_TIMES, --  被叫时长IN_JF_TIMES
E.IN_CNT, --  被叫次数IN_CNT
F.OUT_SMS_NUM, --  OUT_SMS_NUM
'' ws_sms, --  省外短信发送量占比
g.qq_cnt, --  使用QQ号码数量
i.BY_LAC_NUM, --  登陆宾阳县区域内基站数量
i.NOT_BY_LAC_NUM, --  登陆宾阳县区域外基站数量       
b.qn_CDR_NUMS, --区内话单条数
b.qw_CDR_NUMS, --区外话单条数
b.qn_LAC_NUMS, --区内基站数量（剔重）
b.qw_LAC_NUMS, --区外基站数量（剔重）
-- j.area_cells  
k1.total_fee fee1,
k2.total_fee fee2,
k3.total_fee fee3,
k4.total_fee fee4,
k5.total_fee fee5,
k6.total_fee fee6
FROM TP_LH_0626_ROAM_LIST_2 A
LEFT JOIN hy_VOICE_numbers_imei B
ON A.USER_ID = B.USER_ID
-- AND SUBSTR(A.IMEI, 1, 14) = B.IMEI
LEFT JOIN (SELECT T.USER_ID,
SUM(CASE
WHEN T.SMS_DIRECT = '01' THEN
T.CDR_NUM
ELSE
0
END) DIRE_SMS
FROM DWA.DWA_S_D_USE_MB_SMS T
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) C
ON A.USER_ID = C.USER_ID
LEFT JOIN hy_flux_imei D
ON A.USER_ID = D.USER_ID
-- AND SUBSTR(A.IMEI, 1, 14) = D.IMEI
LEFT JOIN hy_voice_jf_times_imei E
ON A.USER_ID = E.USER_ID
LEFT JOIN (SELECT T.USER_ID, SUM(T.OUT_SMS_NUM) OUT_SMS_NUM
FROM DWA.DWA_V_D_CUS_MB_SING_SMS T
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) F
ON A.USER_ID = F.USER_ID
left join dm.mid_roam_qq_sum g
on a.device_number = g.device_number
left join hy_BY_LAC_ID I
on a.user_id = i.user_id 
--left join temp_20171011_xxcell j
--on a.user_id=j.user_id 
left join dwa.DWA_V_M_CUS_mB_CHARGE k1 on a.user_id=k1.user_id and k1.month_id='$v_lmonth_1'
left join dwa.DWA_V_M_CUS_mB_CHARGE k2 on a.user_id=k2.user_id and k2.month_id='$v_lmonth_2'
left join dwa.DWA_V_M_CUS_mB_CHARGE k3 on a.user_id=k3.user_id and k3.month_id='$v_lmonth_3'
left join dwa.DWA_V_M_CUS_mB_CHARGE k4 on a.user_id=k4.user_id and k4.month_id='$v_lmonth_4'
left join dwa.DWA_V_M_CUS_mB_CHARGE k5 on a.user_id=k5.user_id and k5.month_id='$v_lmonth_5'
left join dwa.DWA_V_M_CUS_mB_CHARGE k6 on a.user_id=k6.user_id and k6.month_id='$v_lmonth_6'
;


--cbss 
insert overwrite table TP_LH_0626_ROAM_LIST_4 
SELECT 
B.DEVICE_NUMBER,  --  DEVICE_NUMBER
b1.product_name,
b.product_id,
A.PROVINCE_NAME,  --  PROVINCE_NAME（请单独增加宾阳）
a.province_code,  -- province_code
a.start_date,
a.end_date,
a.stay_days,
C.CITY_NAME,  --  CITY_NAME
c.city_no,  -- city_no
C.AREA_NAME,  --  AREA_NAME
c.area_no,  -- area_no
u.prof_name,
CASE WHEN D.CUST_TYPE_CBSS = '1' THEN '1' ELSE '0' END IS_JK, --  IS_JK
D.REALNAME_FLAG,  --  REALNAME_FLAG
F.comments USER_STATUS_PROV_DESC,  --  用户状态
b.user_status,  -- 用户状态编码
'后付' PAY_MODE_DESC,  --  付费类型
'01' PAY_MODE,  -- 付费类型
D.CUST_NAME,  --  CUST_NAME
D.PSPT_ID,  --  PSPT_ID
D.ID_ADDRESS, --  ID_ADDRESS
E1.CERT_TYPE_DESC CERT_TYPE_PROV_DESC, --  证件类型
e1.cert_type, -- 证件类型
B.DEVELOPER_ID OPER_DEPT_NO, --  OPER_DEPT_NO
C.CHANNEL_NAME, --  CHANNEL_NAME,
b.channel_no, -- channel_no
B.INNET_DATE INNET_DATE, --  INNET_DATE
H.CHANNEL_TYPE_DESC CHANNEL_TYPE,  --  发展渠道类型
h.chnl_type channel_type_no,  -- 发展渠道类型
s.back_dept_desc, --  返档渠道名称,
s.back_dept_no, -- 返档渠道
case when k.pay_charge > '0' then '1' else  '0' end is_pay ,--  入网后是否有缴费记录
A.IMEI, --  IMEI
'' imei_if_hekui, --  IMEI是否规范
''  imei_if_zb,--  IMEI是否为涉嫌诈骗终端
A.IF_ROAM,
A.IF_LOCAL,
A.IF_FOCUS,
A.IF_NORMAL,
A.USER_ID,
a.FLAG_TYPE 
FROM (
SELECT B.USER_ID,
B.IMEI IMEI,
NULL IF_ROAM,
NULL IF_LOCAL,
NULL IF_FOCUS,
NULL IF_NORMAL,
b.PROVINCE_NAME,
b.province_code,
'1' FLAG_TYPE,
start_date,
end_date,
stay_days  
FROM temp_roam_xx_list B
) A
INNER JOIN (SELECT *
FROM DWA.DWA_V_d_CUS_CB_USER_INFO T
WHERE T.MONTH_ID = '$v_month_id'
and t.day_id='$v_day_id'
AND T.SERVICE_TYPE = '40AAAAAA') B
ON A.USER_ID = B.USER_ID
left join  (select b.* from dwd.dwd_d_prd_cb_product b where month_id = '$v_month_id' and day_id = '$v_day_id')  b1
on b.product_id=b1.product_id
LEFT JOIN (SELECT T.CHANNEL_NO,
MAX(T.CITY_NAME) CITY_NAME,
max(t.city_no) city_no,
MAX(T.AREA_NAME) AREA_NAME,
max(t.area_no) area_no,
MAX(T.CHANNEL_NAME) CHANNEL_NAME,
max(CHANNEL_TYPE_ZB) CHANNEL_TYPE_ZB
FROM DWD.DWD_D_PUB_AL_CHANNEL T
WHERE T.MONTH_ID = '$v_month_id'
AND T.DAY_ID = '$v_day_id'
GROUP BY T.CHANNEL_NO) C
ON B.CHANNEL_NO = C.CHANNEL_NO
INNER JOIN (SELECT CUST_ID,
CUST_NAME,
PSPT_TYPE PSPT_ID_TYPE,
PSPT_ID,
PSPT_ADDR ID_ADDRESS,
CASE
WHEN RSRV_TAG1 = '5' THEN
'二代+公安'
WHEN RSRV_TAG1 = '4' THEN
'实名-二代'
WHEN RSRV_TAG1 = '3' THEN
'实名-公安'
WHEN RSRV_TAG1 = '2' OR RSRV_TAG1 IS NULL THEN
'实名-系统'
ELSE
'未实名'
END REALNAME_FLAG,
CUST_TYPE_CBSS
FROM DWD.DWD_D_CUS_CB_CUSTOMER
WHERE MONTH_ID = '$v_month_id'
AND DAY_ID = '$v_day_id') D
ON B.CUST_ID = D.CUST_ID
left join  (SELECT T.CERT_TYPE, T.CERT_TYPE_DESC
FROM DIM.DIM_REF_CERT_TYPE T
GROUP BY T.CERT_TYPE, T.CERT_TYPE_DESC) E1
ON D.PSPT_ID_TYPE = E1.CERT_TYPE
left join (select * from dim.dim_pub_cbss_code_list t where t.column_id = upper('USER_STATUS'))  F
ON b.user_status = F.code_id
left join (SELECT P.CHANNEL_NO, Q.CHANNEL_TYPE_DESC, q.chnl_type
FROM DIM.DIM_PUB_ZB_AGENT_CODE P
LEFT JOIN DIM.DIM_SIX_CHANNEL Q
ON P.CHNL_CODE = Q.CHANNEL_NO) H
on C.CHANNEL_NO = H.CHANNEL_NO
left join mid_roam_pay k
on a.user_id = k.user_id
left join mid_roam_backuser s
on a.user_id=s.user_id 
left join dim.DIM_PUB_CODE_ORG_CHNL u on b.chnl_no=u.chnl_no;


INSERT INTO TABLE roam_user_list_maoming partition (month_id = '$v_month_id',day_id='$v_day_id')
SELECT A.*,
B.ZJ_TIMES, --  ZJ_TIMES
B.ZJ_DURA, --  ZJ_DURA
C.DIRE_SMS, --  DIRE_SMS
-- case
--  when h.user_id is not null then
--  '是'
-- lse
-- '否'
-- end is_transfer_call, --  是否有呼叫转移话单
-- h.OTHER_NUMBER, --  呼叫转移被叫号码
D.TOTAL_FLUX, --  LOCAL_NET
E.OUT_JF_TIMES, --  OUT_JF_TIMES
E.OUT_CNT, --  主叫次数OUT_CNT
E.IN_JF_TIMES, --  被叫时长IN_JF_TIMES
E.IN_CNT, --  被叫次数IN_CNT
F.OUT_SMS_NUM, --  OUT_SMS_NUM
'' ws_sms, --  省外短信发送量占比
g.qq_cnt, --  使用QQ号码数量
i.BY_LAC_NUM, --  登陆宾阳县区域内基站数量
i.NOT_BY_LAC_NUM, --  登陆宾阳县区域外基站数量
b.qn_CDR_NUMS, --区内话单条数
b.qw_CDR_NUMS, --区外话单条数
b.qn_LAC_NUMS, --区内基站数量（剔重）
b.qw_LAC_NUMS, --区外基站数量（剔重）
--j.area_cells ,
k1.total_fee fee1,
k2.total_fee fee2,
k3.total_fee fee3,
k4.total_fee fee4,
k5.total_fee fee5,
k6.total_fee fee6         
FROM TP_LH_0626_ROAM_LIST_4 A
LEFT JOIN hy_VOICE_numbers_imei B
ON A.USER_ID = B.USER_ID
--AND SUBSTR(A.IMEI, 1, 14) = B.IMEI
LEFT JOIN (SELECT T.USER_ID,
SUM(CASE WHEN T.SMS_DIRECT = '01' THEN T.CDR_NUM ELSE 0 END) DIRE_SMS
FROM DWA.DWA_S_D_USE_CB_SMS T
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) C
ON A.USER_ID = C.USER_ID
LEFT JOIN hy_flux_imei D
ON A.USER_ID = D.USER_ID
--AND SUBSTR(A.IMEI, 1, 14) = D.IMEI
LEFT JOIN hy_voice_jf_times_imei E
ON A.USER_ID = E.USER_ID
LEFT JOIN (SELECT T.USER_ID, SUM(T.OUT_SMS_NUM) OUT_SMS_NUM
FROM DWA.DWA_V_D_CUS_CB_SING_SMS T
WHERE T.MONTH_ID = '$v_month_id'
GROUP BY T.USER_ID) F
ON A.USER_ID = F.USER_ID
left join mid_roam_qq_sum g
on a.device_number = g.device_number
left join hy_BY_LAC_ID I
on a.user_id = i.user_id
--left join temp_20171011_xxcell j
--  on a.user_id=j.user_id 
left join dwa.DWA_V_M_CUS_cB_CHARGE k1 on a.user_id=k1.user_id and k1.month_id='$v_lmonth_1'
left join dwa.DWA_V_M_CUS_cB_CHARGE k2 on a.user_id=k2.user_id and k2.month_id='$v_lmonth_2'
left join dwa.DWA_V_M_CUS_cB_CHARGE k3 on a.user_id=k3.user_id and k3.month_id='$v_lmonth_3'
left join dwa.DWA_V_M_CUS_cB_CHARGE k4 on a.user_id=k4.user_id and k4.month_id='$v_lmonth_4'
left join dwa.DWA_V_M_CUS_cB_CHARGE k5 on a.user_id=k5.user_id and k5.month_id='$v_lmonth_5'
left join dwa.DWA_V_M_CUS_cB_CHARGE k6 on a.user_id=k6.user_id and k6.month_id='$v_lmonth_6'
;
"


#hive执行sql命令，并将执行结果写入日志文件中
hive -e "
use $v_user;
" 2>&1 |tee $v_logfile >>/dev/null

echo "$v_sql1;"
echo "  "
hive -e "
use $v_user;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
$v_sql1;" 2>&1 |tee $v_logfile >>/dev/null

v_md = `echo $v_date | cut -c 5-8`
hive -e "set hive.cli.print.header=true;select * from lt_mengguanzhou.roam_user_list_maoming where month_id = '$v_month_id' 
and day_id = '$v_day_id' and province_name in ('广东.茂名', '福建.龙岩', '海南.海口')" > maoming$v_md.txt

python /home/lt_mengguanzhou/cheatPre.py
