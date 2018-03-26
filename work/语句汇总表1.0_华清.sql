-----余华清低消
----BSS-D

--truncate table channel_list_gx 
create table channel_list_gx as 
select d.CHANNEL_NO,d.CHANNEL_NAME from 
(select t.*,row_number()over(partition by t.channel_no order by t.is_only_valid) rn
 from DWD.DWD_D_PUB_AL_CHANNEL t
 where month_id='201801' and day_id='22') d where d.rn<=1;
 
--truncate table mid_d_mb_dixiao
insert into table mid_d_mb_dixiao
SELECT
    A.USER_ID AS USER_ID,
    '0' AS is_ice,--是否办理畅越冰激凌,
    (CASE WHEN T1.SERVICE_NAME is not null THEN '1' ELSE '0' END) AS is_flux_pkg,--是否订购流量包,
    T1.AGENT_NAME  AS FLUX_CHANNEL_TYPE,  --流量包受理渠道,
    T1.DEPT_NO AS FLUX_CHANNEL_ID,--流量包受理渠道编码,
    NULL AS ICE_PRODUCT_ID,--畅越冰激凌产品代码,
    '' AS ICE_ACCEPT_DATE,--畅越冰激凌受理时间,
    '' AS ICE_TRADE_STAFF_ID,--越冰激凌受理工号,
    '' AS ICE_TRADE_DEPART_ID,--畅越冰激凌受理渠道编码,
    '' AS is_cybjl,--是否办理畅越冰激凌,
    '' AS ICE_PRODUCT_NAME,--畅越冰激凌名称,
    '' AS ice_dangwei,--畅越冰激凌档位,
    (CASE WHEN T3.code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') THEN '1' ELSE '0' END) AS IS_LIUSHI,--是否受理流失预警,
    T3.OPER_NO AS LIUSHI_OPER_NO,--流失预警受理工号,
    T3.mkt_sub_title AS LIUSHI_mkt_sub_title,--受理流失预警业务名称,
    T3.code_mkt AS LIUSHI_code_mkt,--受理流失预警营销代码,
    T3.CHANNEL_ID AS LIUSHI_CHANNEL_ID,--流失预警渠道编码,
    T3.CHANNEL_NAME AS LIUSHI_CHANNEL_TYPE,--流失预警受理渠道,
    T7.hy_type HY_TYPE,--续约的合约类型,
    T7.area_name HY_ACCEPT_AREA_NO,--合约续约受理地市,
    T7.XY_OPER_NO HY_oper_no,--合约续约受理工号,
    T7.XY_CHANNEL_NAME HY_CHNL_NAME,--合续约受理渠道,
    T7.XY_CHNL_NO HY_CHNL_CODE,--合约续约渠道编码,
    '' is_auto_renew,--是否自动续约
    T5.OPER_DATE AS DIXIAO_ACCEPT_DATE,--低消受理时间,
    T5.OPER_NO AS DIXIAO_TRADE_STAFF_ID,--低消受理工号,
    T5.CODE_MKT AS DIXIAO_DISCNT_CODE,--受理低消业务代码,
    T5.MKT_SUB_TITLE AS dixiao_product_name,--受理低消业务名称,
    (CASE WHEN T5.CODE_MKT IS NOT NULL THEN '1' ELSE '0' end) AS IS_ACCEPT_DIXIAO, --是否办理低消业务,
    T6.CHANNEL_NO AS DIXIAO_TRADE_DEPART_ID,--低消受理渠道编码,
    T6.CHANNEL_NAME AS DIXIAO_CHANNEL_TYPE--低消受理渠道名称,
 
FROM (SELECT *
 FROM DWA.DWA_V_D_CUS_MB_USER_DERIVE T
 WHERE T.MONTH_ID = '201801' 
 AND T.DAY_ID = '22'
 AND T.IS_INNET = '1') A
 
    LEFT OUTER JOIN 
(select a.user_id,a.SERVICE_CODE,a.SERVICE_NAME, b.DEPT_NO, b.AGENT_NAME 
from (SELECT e.*,row_number() over(partition by user_id order by OPER_DATE desc)as rn FROM DWA.DWA_S_D_USE_MB_PKG_GPRS_USED e WHERE MONTH_ID='201802' AND DAY_ID='28' AND SERVICE_NAME like '%流量包%')a 
left join DIM.DIM_PUB_AGENT b on a.OPER_DEPT_NO=b.DEPT_NO where A.rn<=1) T1 
ON A.USER_ID=T1.USER_ID
 
--    LEFT OUTER JOIN 
--(select * from(
--select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_EVT_MB_MKT_ACTIVITY b where code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') and month_id='201801' and day_id='22' and BEGIN_DATE<='20180122' and END_DATE>='20180122') a  
--where a.rn <=1) T2 
--ON A.USER_ID=T2.USER_ID
 
    LEFT OUTER JOIN 
(select A.user_id,A.CODE_MKT,A.MKT_SUB_TITLE,A.CHANNEL_ID,A.OPER_NO,B.CHANNEL_NAME from
(select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_PRD_MB_MKT_INFO b where code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') and month_id='201801' and day_id='22' and START_DATE<='20180122' and END_DATE>='20180122') A  
left outer join channel_list_gx B ON A.CHANNEL_ID=B.CHANNEL_NO  where A.rn <=1) T3
ON A.USER_ID = T3.USER_ID
  
    LEFT OUTER JOIN 
(SELECT A.USER_ID,A.OPER_DATE,A.OPER_NO,B.CODE_MKT,B.MKT_SUB_TITLE 
  FROM 
    (select * from(
select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_EVT_MB_MKT_ACTIVITY b where month_id='201801' and day_id='22' and BEGIN_DATE <='20180122' and END_DATE>='20180122') a  
where rn <=1) A
 INNER JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT) T5 
ON A.USER_ID=T5.USER_ID
    
    LEFT OUTER JOIN 
 (SELECT E.*, C.CHANNEL_NO,C.CHANNEL_NAME FROM 
  (SELECT A.USER_ID,A.CHANNEL_ID,B.CODE_MKT,B.MKT_SUB_TITLE
  FROM 
  (select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_PRD_MB_MKT_INFO b where month_id='201801' and day_id='22' and START_DATE<='20180122' and END_DATE >='20180122') A
  RIGHT JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT WHERE A.rn <=1) E 
  left outer join channel_list_gx C ON E.CHANNEL_ID=C.CHANNEL_NO) T6 
ON A.USER_ID=T6.USER_ID
LEFT OUTER JOIN (SELECT * FROM dm.DM_D_WX_CL_ZDHY_XY_LIST WHERE MONTH_ID='201801' AND DAY_ID='22') T7 ON A.USER_ID=T7.USER_ID; 



---CB-D
--truncate table mid_d_cb_dixiao1
insert into table mid_d_cb_dixiao1

SELECT 
A.USER_ID AS USER_ID,
'' is_ice, -------(CASE WHEN T1.PRODUCT_ID IS NOT NULL THEN '1' ELSE '0' END) AS is_ice,--是否办理畅越冰激凌,
(CASE WHEN T3.FLUX_PKG IS NOT NULL THEN '1' ELSE '0' END) AS is_flux_pkg, --是否订购流量包,
T3.CHNL_NAME  AS FLUX_CHANNEL_TYPE, ---流量包受理渠道,
T3.CHANNEL_ID AS FLUX_CHANNEL_ID,---流量包受理渠道编码,
T4.PRODUCT_ID AS ICE_PRODUCT_ID,--畅越冰激凌产品代码,
T4.ACCEPT_DATE AS ICE_ACCEPT_DATE,--畅越冰激凌受理时间,
T4.TRADE_STAFF_ID AS ICE_TRADE_STAFF_ID,--越冰激凌受理工号,
T4.TRADE_DEPART_ID AS ICE_TRADE_DEPART_ID,--畅越冰激凌受理渠道编码,
CASE WHEN T4.PRODUCT_ID IS NOT NULL THEN '1' ELSE '0' end is_cybjl --是否办理畅越冰激凌,
FROM (SELECT *
FROM DWA.DWA_V_D_CUS_CB_USER_INFO T
WHERE T.MONTH_ID = '201801' 
AND T.DAY_ID = '22'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN (SELECT D.USER_ID,D.CHANNEL_ID,D.FLUX_PKG,E.CHNL_NAME
FROM 
(SELECT T.USER_ID,C.CHANNEL_ID,C.FLUX_PKG
FROM (select b.*,row_number() over(partition by user_id order by ACCEPT_TIME desc) as rn from DWA.DWA_V_D_CUS_CB_DISCNT_INFO b where month_id='201801' and day_id='22' and START_DATE <='20180122' and END_DATE >='20180122') T
inner join (select USER_ID,PRODUCT_ID,PACKAGE_ID,FLUX_PKG,CHANNEL_ID from DWA.DWA_V_D_CUS_CB_DIY_PACKAGE 
WHERE MONTH_ID = '201801' 
AND DAY_ID = '22' 
AND FLUX_PKG IS NOT NULL) C ON T.USER_ID=C.USER_ID WHERE T.rn <=1) D
left outer join dim.DIM_PUB_AGENT_ZB_REL E ON D.CHANNEL_ID=E.CHNL_CODE WHERE E.MAIN_FLAG='1')T3 
ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN
  (select * from (select b.*,row_number() over(partition by user_id order by ACCEPT_DATE desc) as rn from DWD.DWD_D_EVT_CB_TRADE_PRODUCT b where month_id='201801' and day_id='22' AND START_DATE <='20180122' AND END_DATE>='20180122' AND PRODUCT_ID IN('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) c where c.rn<=1) T4
ON A.USER_ID=T4.USER_ID;


--truncate table mid_d_cb_dixiao2
insert into table mid_d_cb_dixiao2

select
a.user_id,
T5.PRODUCT_NAME AS ICE_PRODUCT_NAME,--畅越冰激凌名称,
(substr(T5.PRODUCT_NAME, instr(T5.PRODUCT_NAME, '-') + 1, instr(T5.PRODUCT_NAME, '元') - instr(T5.PRODUCT_NAME, '-') - 1)) AS ice_dangwei,--畅越冰激凌档位,
'' AS IS_LIUSHI,--是否受理流失预警,
'' AS LIUSHI_OPER_NO, --流失预警受理工号,
'' AS LIUSHI_mkt_sub_title,--受理流失预警业务名称,
'' AS LIUSHI_code_mkt, --受理流失预警营销代码,
'' AS LIUSHI_CHANNEL_ID, --流失预警渠道编码,
'' AS LIUSHI_CHANNEL_TYPE, --流失预警受理渠道,
T8.hy_type HY_TYPE,--续约的合约类型,
T8.area_name HY_ACCEPT_AREA_NO,--合约续约受理地市,
T8.XY_OPER_NO HY_oper_no,--合约续约受理工号,
T8.XY_CHANNEL_NAME HY_CHNL_NAME,--合续约受理渠道,
T8.XY_CHNL_NO HY_CHNL_CODE,--合约续约渠道编码,
'' is_auto_renew,--是否自动续约
T7.ACCEPT_DATE AS DIXIAO_ACCEPT_DATE,--低消受理时间,
T7.TRADE_STAFF_ID AS DIXIAO_TRADE_STAFF_ID,--低消受理工号,
T7.DISCNT_CODE AS DIXIAO_DISCNT_CODE,--受理低消营销代码,
(CONCAT(T7.MKT_SUB_TITLE,T7.DISCNT_NAME)) AS dixiao_product_name,--受理低消业务名称,
(CASE WHEN T7.DISCNT_CODE IS NOT NULL THEN 1 ELSE 0 end) AS IS_ACCEPT_DIXIAO, --是否办理低消业务,
T7.TRADE_DEPART_ID AS DIXIAO_TRADE_DEPART_ID,--低消受理渠道编码,
T7.CHNL_NAME AS DIXIAO_CHANNEL_TYPE--低消受理渠道名称
FROM (SELECT user_id
FROM DWA.DWA_V_D_CUS_CB_USER_INFO T
WHERE T.MONTH_ID = '201801' 
AND T.DAY_ID = '22'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN 
(SELECT A.USER_ID,A.PRODUCT_ID,B.PRODUCT_NAME FROM
   (select b.*,row_number() over(partition by user_id order by ACCEPT_DATE desc) as rn from DWD.DWD_D_EVT_CB_TRADE_PRODUCT b where month_id='201801' and day_id='22' AND START_DATE <='20180122' AND END_DATE>='20180122' AND PRODUCT_ID IN ('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) A
        LEFT OUTER JOIN
        (SELECT PRODUCT_ID,PRODUCT_NAME FROM DWD.DWD_D_PRD_CB_PRODUCT WHERE MONTH_ID = '201801' AND DAY_ID = '22' AND START_DATE<='20180122' AND END_DATE >='20180122') B 
         ON A.PRODUCT_ID=B.PRODUCT_ID where A.rn<=1) T5
ON A.USER_ID=T5.USER_ID
LEFT OUTER JOIN
(SELECT D.*, C.CHNL_NAME
FROM (SELECT A.ID AS user_id,A.ACCEPT_DATE,A.TRADE_DEPART_ID,A.TRADE_STAFF_ID,B.PRODUCT_ID,B.PACKAGE_ID,B.DISCNT_CODE,B.MKT_SUB_TITLE,B.DISCNT_NAME 
   FROM 
   (SELECT b.*, row_number() over(partition by id ORDER BY accept_date desc ) as rn  FROM DWD.DWD_D_EVT_CB_TRADE_DISCNT b WHERE MONTH_ID='201801' AND DAY_ID='22') A 
     INNER JOIN DIXIAO_CB_MKT B 
     ON A.PRODUCT_ID=B.PRODUCT_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.DISCNT_CODE=B.DISCNT_CODE WHERE A.rn<=1) D
     LEFT OUTER JOIN dim.DIM_PUB_AGENT_ZB_REL C ON D.TRADE_DEPART_ID=C.CHNL_CODE WHERE C.MAIN_FLAG='1') T7 
ON A.USER_ID=T7.user_id
LEFT OUTER JOIN (SELECT * FROM dm.DM_D_WX_CL_ZDHY_XY_LIST WHERE MONTH_ID='201801' AND DAY_ID='22') T8 ON A.USER_ID=T8.USER_ID;



     
-----低销CB日合并
--truncate table mid_d_cb_dixiao
insert into table mid_d_cb_dixiao
select 
a.*,
b.ICE_PRODUCT_NAME,
b.ice_dangwei,
b.IS_LIUSHI,
b.LIUSHI_OPER_NO,
b.LIUSHI_mkt_sub_title,
b.LIUSHI_code_mkt,
b.LIUSHI_CHANNEL_ID,
b.LIUSHI_CHANNEL_TYPE,
b.HY_TYPE,
b.HY_ACCEPT_AREA_NO,
b.HY_oper_no,
b.HY_CHNL_NAME,
b.HY_CHNL_CODE,
b.is_auto_renew,
b.DIXIAO_ACCEPT_DATE,
b.DIXIAO_TRADE_STAFF_ID,
b.DIXIAO_DISCNT_CODE,
b.dixiao_product_name,
b.IS_ACCEPT_DIXIAO,
b.DIXIAO_TRADE_DEPART_ID,
b.DIXIAO_CHANNEL_TYPE
from mid_d_cb_dixiao1 a
left join mid_d_cb_dixiao2 b
on a.user_id=b.user_id;



--CB-M
--truncate table mid_m_cb_dixiao
insert into table mid_m_cb_dixiao

SELECT A.USER_ID,
    '' PURCH_MODE_TYPE_DESC,
  CONCAT(T2.MKT_SUB_TITLE, T2.DISCNT_NAME) as dixiao_type, --低消活动类型,
  CASE WHEN T2.DISCNT_NAME IS NOT NULL THEN '1' ELSE '0' end is_promise_dixiao,--是否承诺低消系列用户
  CASE WHEN T3.DISCNT_CODE IS NOT NULL THEN '1' ELSE '0' end is_participate_dixiao, --是否有参加营销活动,
  T3.DISCNT_NAME as yingxiao_product_name, --营销活动名称,
  CASE WHEN T4.FLUX_PKG IS NOT NULL THEN '1' ELSE '0' end is_pkg_user, --是否流量包用户,
  CASE  WHEN (T5.PURCH_MODE_CODE IS NOT NULL OR T5.LOG_USER_MKT_SN IS NOT NULL) THEN '1' ELSE '0' end is_auto --是否自动续约
  FROM (SELECT *
            FROM DWA.DWA_V_M_CUS_CB_USER_INFO T
   WHERE T.MONTH_ID = '201801'
             AND T.IS_INNET = '1'
             AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN (SELECT A.ID AS user_id,A.ACCEPT_DATE,B.PRODUCT_ID,B.PACKAGE_ID,B.DISCNT_CODE,B.MKT_SUB_TITLE,B.DISCNT_NAME 
   FROM 
   (SELECT b.*, row_number() over(partition by id ORDER BY accept_date desc ) as rn  FROM DWD.DWD_D_EVT_CB_TRADE_DISCNT b WHERE MONTH_ID='201801' AND DAY_ID='31' AND START_DATE<='20180122' AND END_DATE>='20180122') A 
     LEFT OUTER JOIN DIXIAO_CB_MKT B 
     ON A.PRODUCT_ID=B.PRODUCT_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.DISCNT_CODE=B.DISCNT_CODE  WHERE A.rn<=1) T2
ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN  (SELECT A.USER_ID,A.PRODUCT_ID, A.PACKAGE_ID,A.DISCNT_CODE,B.DISCNT_NAME FROM
 (select b.*,row_number() over(partition by user_id order by start_date desc) as rn from dwd.DWD_D_PRD_CB_USER_DISCNT b WHERE MONTH_ID='201801' AND DAY_ID = '31' and end_date>='20180131' and
  start_date<='20180131') A 
 LEFT OUTER JOIN (SELECT DISCNT_CODE,DISCNT_NAME FROM zbg_src.SRC_D_BCD07003 WHERE MONTH_ID ='201803' and day_id='17' and discnt_name not 
 like '%开通4G%') B ON A.DISCNT_CODE=B.DISCNT_CODE where A.rn<=1) T3 
 ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN (SELECT T.USER_ID,C.CHANNEL_ID,C.FLUX_PKG
FROM (select b.*,row_number() over(partition by user_id order by ACCEPT_TIME desc) as rn from DWA.DWA_V_D_CUS_CB_DISCNT_INFO b where month_id='201801' and day_id='31' and START_DATE <='20180131' and END_DATE >='20180131') T
inner join (select USER_ID,PRODUCT_ID,PACKAGE_ID,FLUX_PKG,CHANNEL_ID from DWA.DWA_V_D_CUS_CB_DIY_PACKAGE 
WHERE MONTH_ID = '201801' 
AND DAY_ID = '31' 
AND FLUX_PKG IS NOT NULL) C ON T.USER_ID=C.USER_ID WHERE T.rn <=1)T4
 on A.USER_ID=T4.USER_ID
LEFT OUTER JOIN (SELECT distinct B.USER_ID,C.PURCH_MODE_CODE,D.LOG_USER_MKT_SN
FROM (SELECT DISTINCT A.USER_ID,B.PURCH_MODE_CODE,B.SERVICE_SN 
FROM DM.MID_D_WX_CL_ZDHY_XY_LIST_P2 A
   INNER JOIN DM.MID_D_WX_CL_ZDHY_XY_LIST_X2 B
  ON A.USER_ID = B.USER_ID WHERE B.PURCH_MODE_CODE not like '%MKT%')B
   LEFT JOIN DM.MID_D_WX_CL_ZDHY_MODE_CODE C
  ON B.PURCH_MODE_CODE = C.PURCH_MODE_CODE
   LEFT JOIN (SELECT DISTINCT A.LOG_USER_MKT_SN
 FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY A
    WHERE CODE_MKT IN ('MKT032001',
   'MKT032002',
   'MKT032003',
   'MKT032004',
   'MKT032005',
   'MKT032006')
  AND A.MKT_LOG_TYPE IN ('10', '20')
  AND A.MONTH_ID = '201802'
  AND A.DAY_ID = '28') D
   ON B.SERVICE_SN = D.LOG_USER_MKT_SN) T5
ON A.USER_ID=T5.USER_ID;


                  
                  
---bss-M
--truncate table mid_m_mb_dixiao
insert into table mid_m_mb_dixiao


SELECT A.USER_ID,
  T1.PURCH_MODE_TYPE_DESC, --本地合约类型,
  T2.MKT_SUB_TITLE as dixiao_type, --低消活动类型,
  CASE WHEN T2.MKT_SUB_TITLE IS NOT NULL THEN '1' ELSE '0' end is_promise_dixiao, --是否承诺低消系列用户,
  CASE WHEN T3.CODE_MKT IS NOT NULL THEN '1' ELSE '0' end is_participate_dixiao, --是否有参加营销活动,
  T3.MKT_SUB_TITLE as yingxiao_product_name, --营销活动名称,
  CASE WHEN T4.is_service_mult_01 ='1' THEN '1' ELSE '0' end is_pkg_user, --是否流量包用户,
  CASE  WHEN (T5.PURCH_MODE_CODE IS NOT NULL OR T5.LOG_USER_MKT_SN IS NOT NULL) THEN '1 ELSE 0' end is_auto --是否自动续约
FROM (SELECT x.*
            FROM (SELECT *
                    FROM DWA.DWA_V_M_CUS_MB_USER_DERIVE T
                   WHERE T.MONTH_ID = '201801'
                     AND T.IS_INNET = '1') x) A
LEFT OUTER JOIN (select A.USER_ID,A.act_mkt_type,B.PURCH_MODE_TYPE_DESC,A.END_DATE
   from (select d.*,row_number() over(partition by user_id order by START_DATE desc) as rn
    from DWD.DWD_D_PRD_MB_ACT_INFO d 
    WHERE MONTH_ID = '201801' 
    AND DAY_ID = '31' 
    and START_DATE <='20180131'
    and END_DATE>='20180131'
    and act_mkt_type is not null) A
    LEFT OUTER JOIN DIM.DIM_PUB_PURCH_MODE_TYPE B 
    ON A.act_mkt_type=B.PURCH_MODE_TYPE WHERE A.rn<=1) T1
   ON A.USER_ID=T1.USER_ID
LEFT OUTER JOIN 
(select A.user_id,B.CODE_MKT, B.MKT_SUB_TITLE FROM
 (select e.*,row_number() over(partition by user_id order by OPER_DATE desc) as rn 
 from dwd.DWD_D_EVT_MB_MKT_ACTIVITY e
 WHERE MONTH_ID='201801' AND DAY_ID = '31' AND BEGIN_DATE<='20180131' AND END_DATE>='20180131') A
   left outer join DIXIAO_MB_MKT B on A.CODE_MKT=B.CODE_MKT where A.rn<=1) T2 
   ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN (SELECT A.USER_ID,A.CODE_MKT,A.MKT_SUB_TITLE FROM 
     (SELECT b.*, row_number() over(partition by USER_ID ORDER BY OPER_DATE desc ) as rn FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY b WHERE MONTH_ID='201801' AND DAY_ID = '31' AND BEGIN_DATE<='20180122' AND END_DATE>='20180122') A WHERE A.rn<=1) T3 --41363008(搞定)
   ON A.USER_ID = T3.USER_ID
LEFT OUTER JOIN (select user_id, max(is_service_mult) as is_service_mult_01 from DWA.DWA_V_M_USE_MB_PKG_USED_INFO WHERE MONTH_ID='201801' group by user_id) T4 --45003526(搞定)
   ON A.USER_ID=T4.USER_ID
LEFT JOIN (SELECT DISTINCT A.USER_ID,C.PURCH_MODE_CODE,D.LOG_USER_MKT_SN
FROM DM.MID_D_WX_CL_ZDHY_XY_LIST_P2 A
   INNER JOIN DM.MID_D_WX_CL_ZDHY_XY_LIST_X2 B
  ON A.USER_ID = B.USER_ID
   LEFT JOIN DM.MID_D_WX_CL_ZDHY_MODE_CODE C
  ON B.PURCH_MODE_CODE = C.PURCH_MODE_CODE
   LEFT JOIN (SELECT DISTINCT A.LOG_USER_MKT_SN
 FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY A
    WHERE CODE_MKT IN ('MKT032001',
   'MKT032002',
   'MKT032003',
   'MKT032004',
   'MKT032005',
   'MKT032006')
  AND A.MKT_LOG_TYPE IN ('10', '20')
  AND A.MONTH_ID = '201801'
  AND A.DAY_ID = '31') D
   ON B.SERVICE_SN = D.LOG_USER_MKT_SN) T5
ON A.USER_ID=T5.USER_ID;
      
---合并
--truncate table mid_m_al_dixiao      
insert into table mid_m_al_dixiao
select * from (
select * from mid_m_mb_dixiao
union all
select * from mid_m_cb_dixiao)c

--truncate table mid_d_al_dixiao      
insert into table mid_d_al_dixiao
select * from (
select * from mid_d_mb_dixiao
union all
select * from mid_d_cb_dixiao)c


