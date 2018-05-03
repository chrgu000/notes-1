-----张日鑫，黎振霖
insert into table lt_zhangrixin.dm_d_al_waihu_user1

select a.user_no,
b.is_guangxi,
c.USE_ID_TYPE,
d.is_cybjl,
e.std_charge,
f.P2P_FEE,
g.MMS_FEE,
h.pay_charge,
i.virtual_number,
j.ACCESS_SPEED,
k.is_lost,
l.is_kd_mb,
m1.pay_charge_day,
m.OPER_NO as wx_OPER_NO,
n.manager_desc as wx_manager_desc,
o.is_yuyue,
p.CREDIT_NUMBER,
q.six_m_arpu,
r.thr_m_max,
s.is_black,
t.is_red,
u.imei,
v.34_msisdn,
w.is_tuoshou
from (select user_no,month_id,day_id,cust_id,device_number from dwd.DWD_d_PRD_AL_USER_INFO where month_id='201801' and day_id='22' and flag='0') a
left join (select b.user_id,
            case when c.user_id is not null then '1' else '0' end is_guangxi 
            from dwa.DWA_V_M_CUS_MB_USER_DERIVE b 
            LEFT OUTER JOIN (select user_id from dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST a where a.month_id='201801' and a.day_id='22' 
            and  a.USE_ID_TYPE='ID001' and a.use_id_no like '45%') c 
            on b.user_id=c.user_id where b.month_id='201801' and b.is_innet='1') b ----------是否广西用户
on a.user_no=b.user_id
left join (select o.USER_ID,o.USE_ID_TYPE 
                    from (select a.USER_ID as USER_ID,a.MONTH_ID as MONTH_ID,b.USE_ID_TYPE as USE_ID_TYPE from dwa.DWA_V_M_CUS_CB_USER_INFO  a 
                          LEFT OUTER JOIN dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST b 
                          on a.USER_ID = b.USER_ID
                          where a.month_id='201801' and b.month_id='201801' and b.day_id='22'
                          union all 
                          select DISTINCT n.USER_ID as USER_ID,
                          n.MONTH_ID as MONTH_ID,m.USE_ID_TYPE as USE_ID_TYPE from dwa.DWA_V_M_CUS_MB_USER_DERIVE  n 
                          LEFT OUTER JOIN dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST m on n.USER_ID = m.USER_ID where n.month_id='201801' 
                          and m.month_id='201801' and m.day_id='22') o 
                    where o.MONTH_ID='201801') c -------------入网证件类型
         on a.user_no=c.user_id
         left join (select month_id,user_id,
                    case when user_id is not null then '1'
                    else '0' end is_cybjl,
                    product_id from ods.ODS_D_PRD_CB_USER 
                    where product_id in ('90311370', '90311373', '90343397', '90343398',
                                         '90343399', '90343400', '90343401', '90343402', 
                                         '90343403', '90343404', '90343405', '90343406', 
                                         '90343407', '90343408', '90343409', '90343410', 
                                         '90343411', '90343412', '90343420', '90343465', 
                                         '90343469', '90343471', '90343475') and month_id ='201801' and day_id='22') d --------畅越冰激凌用户标识
         on a.user_no=d.user_id
         left join (select a.USER_NO,b.std_charge from dwd.DWD_M_PRD_AL_USER_INFO a 
                    LEFT OUTER JOIN (select std_charge,product_id from dim.DIM_REF_PRODUCT_TYPE) b
                    on a.PRODUCT=b.product_id 
                    where a.month_id='201801') e ----------主套餐固定费用
         on a.user_no=e.user_no 
         left join (select f.USER_NO,t.P2P_FEE 
                    from (select user_no from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801') f 
                    LEFT OUTER JOIN (select USER_ID as USER_ID,P2P_FEE as P2P_FEE from DWA.DWA_V_M_CUS_CB_CHARGE where month_id='201801'
                                     union all select USER_ID as USER_ID,P2P_FEE as P2P_FEE from DWA.DWA_V_M_CUS_MB_CHARGE where month_id='201801') t
                    on f.USER_NO=t.user_id) f ----------发送短信费用
         on a.user_no=f.user_no
         left join (select f.USER_NO,t.MMS_FEE from 
                    (select user_no from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801') f 
                    LEFT OUTER JOIN (select USER_ID as USER_ID,MMS_FEE as MMS_FEE from DWA.DWA_V_M_CUS_CB_CHARGE where month_id='201801'
                                     union all select USER_ID as USER_ID,MMS_FEE as MMS_FEE from DWA.DWA_V_M_CUS_MB_CHARGE where month_id='201801' ) t
                    on f.USER_NO=t.USER_ID) g ---------发送彩信费用
         on a.user_no=g.user_no
         left join (select user_id,pay_charge,month_id from dwd.DWD_D_ACC_AL_PAY where month_id='201801' and day_id='22') h ------缴费金额-----重复
         on a.user_no=h.user_id
         left join (select M.DEVICE_NUMBER as virtual_number,M.user_no from  
                  (select user_no,DEVICE_NUMBER from dwd.DWD_D_PRD_AL_USER_INFO where month_id='201801' and day_id='22') M 
                    inner join(SELECT B.USER_ID
                               FROM (SELECT *
                                     FROM DWA.DWA_V_D_CUS_AL_COMB T1 
                                     WHERE T1.MONTH_ID ='201801'
                                     AND T1.DAY_ID = '22'
                                     AND T1.IS_COMP_VALID = '1') A
                               INNER JOIN (SELECT *
                                           FROM DWA.DWA_V_D_CUS_AL_COMB_MEMBERS T1
                                           WHERE T1.MONTH_ID ='201801'
                                           AND T1.DAY_ID = '22'
                                           AND T1.IS_VALID = '1') B
                                           ON A.COMP_ID = B.COMP_ID) N 
                   on M.USER_NO=N.USER_ID) i  -----------虚拟号码
         on a.user_no=i.user_no
         left join (select M.USER_NO,N.ACCESS_SPEED from (select user_no from dwd.DWD_D_PRD_AL_USER_INFO where month_id='201801' and day_id='22') M 
                    left JOIN (select user_id,ACCESS_SPEED from DWD.DWD_M_PRD_FX_USER_INFO where month_id='201801') N
                    on M.USER_NO = N.USER_ID) j ---------可装机最大速率
         on a.user_no=j.user_no
         left join (select a.user_id,a.is_lost from 
                      (select user_id,is_lost from dwa.DWA_V_M_CUS_MB_USER_DERIVE where is_lost='1' and month_id='201801'
                       union all
                       select user_id,is_lost from dwa.DWA_V_M_CUS_FX_USER_DERIVE where is_lost='1' and month_id='201801'
                       union all
                       select user_id,is_lost from dwa.DWA_V_M_CUS_CB_USER_INFO where is_lost='1' and month_id='201801') a) k ----------当月流失用户标识   
         on a.user_no=k.user_id
         left join (select  distinct service_sn,USER_ID,case when MKT_SUB_TITLE is not null then '1' else '0' end is_kd_mb
                    from dwd.DWD_D_PRD_MB_MKT_INFO  
                    where month_id='201801' and 
                    substr(END_DATE, 1, 10)<'201801'
                    and MKT_SUB_TITLE like '%宽带送手机%' and day_id='22') l   ------------------存宽带送手机用户
         on a.user_no=l.user_id
         left join (select user_id,count(distinct wrtoff_date) as pay_charge_day from dwd.DWD_D_ACC_AL_PAY_EXT where month_id='201801'
          and day_id='22' group by user_id) m1 ---------缴费天数（当月有缴费的天数）
         on a.user_no=m1.user_id
         left join (select b.user_id,b.OPER_NO from dwd.DWD_D_CUS_AL_AGENT_USER_REL b where month_id='201801' and day_id='22') m --------维系工号
         on a.user_no=m.user_id
         left join (select manager_desc,user_id from dwd.DWD_D_CUS_AL_AGENT_USER_REL a
                    LEFT OUTER JOIN 
                    dim.DIM_PUB_JZYX_MANAGER_NO b on a.OPER_NO=b.manager_no where a.MONTH_ID='201801' and a.day_id='22') n ---------维系经理名字
         on a.user_no=n.user_id
         left join (select USER_ID,case when 
                    NEXT_DEAL_FLAG not in ('9','E','F') and cancel_flag='0' then '1'
                    else '0' end is_yuyue from dwd.DWD_D_EVT_CB_TRADE_HIS where month_id='201801' and day_id='22'
                    union all
                    select user_id,case when substr(concat(substr(REAL_CHANGE_DATE, 1, 4),substr(REAL_CHANGE_DATE, 6, 7)),1,6)>'201801' then '1'
                    else '0' end is_yuyue
                    from dwd.DWD_D_EVT_AL_USER_SER_DINNER where REAL_CHANGE_DATE is not null 
                    and month_id='201801' and day_id='22') o --------------是否存在预约工单
         on a.user_no=o.user_id
         left join (select MONTH_ID,USER_ID,CREDIT_NUMBER from dwa.DWA_V_D_CUS_MB_USER_DERIVE where MONTH_ID='201801' and day_id='22'
                    union all
                    select MONTH_ID,USER_ID,CREDIT_VALUE as CREDIT_NUMBER from dwa.DWA_V_M_CUS_CB_USER_INFO where MONTH_ID='201801') p ---------用户可用信用额度
         on a.user_no=p.user_id
         left join (select  USER_NO,avg(ACCT_CHARGE) as six_m_arpu from dwd.DWD_M_ACC_AL_CHARGE 
                    where MONTH_ID in ('201801','201801'-1,'201801'-2,'201801'-3,'201801'-4,'201801'-5) group by  USER_NO) q ----------近六个月ARPU
         on a.user_no=q.user_no
         left join (select USER_NO,max(ACCT_CHARGE) as thr_m_max from dwd.DWD_M_ACC_AL_CHARGE
                     where MONTH_ID in ('201801','201801'-1,'201801'-2) group by USER_NO) r --------近三个月最低费用
         on a.user_no=r.user_no
         left join (select customer_no,case when
                     customer_no is not null then '1' 
                     else '0' end is_black
                     from dwd.DWD_D_CUS_AL_BLACK_LIST where month_id='201801' and customer_no is not null and day_id='22') s --------是否黑名单
         on a.cust_id=s.customer_no
         left join  ( select user_id,case when user_id is not null then '1'
                      else '0' end is_red from DWD.DWD_D_CUS_AL_VIP where month_id='201801' and user_id is not null and day_id='22') t --------是否红名单
         on a.user_no=t.user_id
         left join (select b.imei,a.user_id from dwa.DWA_V_M_CUS_MB_USER_DERIVE a LEFT OUTER JOIN 
                           (select imei,msisdn from dwd.DWD_M_RES_MB_TUPLE_FIVE where month_id='201801') b 
                            on a.DEVICE_NUMBER=b.msisdn
                            where a.month_id='201801' and a.IS_INNET='1') u -----------终端串号
         on a.user_no=u.user_id
         left join (select s.msisdn as 34_msisdn from 
                          (select substr(a.imei, 1, 8) as imei_pre,msisdn from dwd.DWD_M_RES_MB_TUPLE_FIVE a where month_id='201801') s
                    left semi join 
                   (select substr(b.imei, 1, 8) as imei_pre,unicom_4g,unicom_3g,unicom_2g from dim.DIM_PUB_FT_IMEI b where unicom_4g='1' or unicom_3g='1' ) t 
                    on s.imei_pre=t.imei_pre) v                          ------3,4G终端用户的设备号码
         on a.DEVICE_NUMBER=v.34_msisdn
         left join (select a.USER_NO,case when b.pay_type in ('003','004','013','020','054','055','068','069','C01','B03','C07','C31','G02','G03','WZF')
                    then '1' else '0' end is_tuoshou 
                    from dwd.DWD_M_PRD_AL_USER_INFO a 
                    LEFT OUTER JOIN (select user_id,pay_type from dwd.DWD_D_ACC_AL_ACCT where month_id='201801' and day_id='22') b
                    on a.USER_NO=b.user_id  where a.month_id='201801') w  ------托收用户标识，1为托收,0为现金和其他cb没有托收
         on a.user_no=w.user_no
         where a.month_id='201801' and a.day_id='22'
         
         
insert into table dm_d_al_waihu_user3

select a.user_no,         
x.special_flag,
y.is_town,
t1.registertime,
t2.complain_sum,
t3.U_NET_TYPE,
t4.TAKE_PICTURE_TAG,
t5.czjzd_chnl_name,
t5.czgzd_chnl_name,
t5.czyyt_name,
t6.is_sch_lac,
'' PROFIT_UNIT,-----y1.PROFIT_UNIT,
y2.bjl_user
from (select user_no,month_id,cust_id,day_id,device_number from dwd.DWD_d_PRD_AL_USER_INFO where month_id='201801' and day_id='22' and flag='0') a
         left join (select A.user_id,case when C.product_id in ('G6080','G6093','90064126','90259327','OS050')
                    or C.product_name like 'M2M%' or B.industry_flag=1 then '1' else '0' end special_flag 
                    from dwa.DWA_V_M_CUS_MB_USER_DERIVE A 
                    LEFT OUTER JOIN dim.DIM_PUB_PRODUCT_ID B
                    on A.product_id=B.product
                    LEFT OUTER JOIN dim.DIM_REF_PRODUCT_TYPE C on A.product_id=C.product_id where A.month_id='201801' and is_innet='') x --------特殊用户标识（ESIM、物联网、隐私号、行业应用）
        on a.user_no=x.user_id
        left join (select USER_NO,a.channel_no,case when b.area_kind in ('02','03') then '1' else '0' end is_town
                   from dwd.DWD_M_PRD_AL_USER_INFO a 
                   LEFT OUTER JOIN (select channel_no,area_kind from dwd.DWD_M_PUB_AL_CHANNEL where month_id='201801' and is_valid='1') b
                   on a.channel_no= b.channel_no where a.month_id='201801' and a.flag='0') y -------是否乡镇属性
        on a.user_no=y.user_no
        left join (select a.user_id,a.tel,b.registertime from
                  (select user_id,DEVICE_NUMBER as tel from dwa.DWA_V_M_CUS_MB_USER_DERIVE where IS_INNET='1' and MONTH_ID='201801') a 
                   LEFT OUTER JOIN (select msisdn as tel,registertime from dwd.DWD_M_RES_MB_TUPLE_FIVE where month_id='201801') b
                   on a.tel=b.tel) t1 ---------设备号码对应的换机时间
        on a.user_no=t1.user_id
        left join (select a.USER_ID,count(distinct a.USER_ID) as complain_sum,b.COMPLAINTS_TYPE from dwd.DWD_D_EVT_AL_V_CONATACTINFO a
                   left join 
                   (select user_no, COMPLAINTS_TYPE from ods.ODS_D_EVT_AL_V_CONATACTINFO where month_id='201801' and day_id='22') b 
                   on  a.user_id=b.user_no where a.month_id='201801' and a.day_id='22' group by a.user_id,b.COMPLAINTS_TYPE) t2   --------累计投诉，投诉类型
        on a.user_no=t2.user_id
        left join (select U_NET_TYPE,user_no from dwd.DWD_M_PRD_MB_NET_INFO where month_id='201801') t3 -----------当月登网类型（月）
        on a.user_no=t3.user_no
        left join (select c.*,
                   case when
                   c.user_no is not null then '1'
                   else '0' end
                   TAKE_PICTURE_TAG
                   from (
                   select a.user_no,a.month_id from dwd.DWD_M_PRD_AL_USER_INFO a left join ods.log_info_lease b on a.user_no=b.user_no
                    where a.month_id='201801' and a.flag='0') c) t4 -----存量拍照用户，1为是，0为否
        on a.user_no=t4.user_no
        left join (select distinct user_id,c.czjzd_chnl_name,c.czgzd_chnl_name,c.czyyt_name from 
                    (select a.* from dm.dm_m_user_zb_tag a 
                    left join 
                    (select distinct user_id from dwa.DWA_S_D_USE_MB_BS  where ROAM_TYPE='01AA' and month_id='201801' and day_id='22' 
                      union all 
                      select distinct user_id from dwa.DWA_S_M_USE_CB_FLUX b where ROAM_TYPE='01AA' and  month_id='201801') b
                    on a.user_id=b.user_id where a.month_id='201801') c) t5  ------------用户居住地最近的自营厅(不跨地市),用户工作地最近的自营厅(不跨地市),用户常驻地营业厅（不跨地市）（月）
        on a.user_no=t5.user_id
        left join (select c.user_no,c.lac,
                   case when 
                   c.lac is not null then '1'
                   else '0' end
                   is_sch_lac
                   from (select b.month_id,b.user_no,b.lac,b.ci from dm.DM_D_RPT_SCH_INFO_RT b
                   left join DM.MAN_JKSCH_CODE a
                   on a.ci=b.ci
                   and a.lac=b.lac
                   where b.month_id='201801' and b.day_id='22') c) t6 ----------- 用户场景位置标识（是否校园）
       on a.user_no=t6.user_no
------       left join (select distinct a.user_id,a.cell_id,b.PROFIT_UNIT from dwa.DWA_S_D_USE_MB_BS a
-----                  left join (select b.PROFIT_UNIT,b.cell_id from dwd.DWD_D_RES_AL_BS_INFO b where month_id='201801' and day_id='22') b 
-------                  on a.cell_id=b.cell_id
--------                  where a.month_id='201801' and a.day_id='22') y1 --------用户位置标识
-------       on a.user_no=y1.user_id
       left join (select user_no,case when user_no is not null then '1' else '0' end bjl_user from dm.DM_M_ALLDM_ICREAM_ACT_M
                  where month_id='201801') y2 ---------老用户全国冰激凌套餐目标用户(月)
       on a.user_no=y2.user_no
       where a.month_id='201802' and a.day_id='22'
       
       


----蒙冠州话务表

--目标表：DWA_A_D/M_PRO_MB_USER、DWA_A_D/M_PRO_CB_USER

--中间表 语音MID_M_MB_VOICE_S
INSERT INTO MID_M_MB_VOICE_S

SELECT
MONTH_ID,user_id,
sum(CASE WHEN TOTAL_FEE > 0 THEN CALL_DURATION
ELSE 0 END) AS VOICE_OUT,
SUM(CDR_NUMS) AS CALL_TIMES,
sum(CASE WHEN CALL_TYPE = '01' THEN CDR_NUMS
ELSE 0 END) AS CALLING_TIMES
FROM
DWA.DWA_S_M_USE_MB_VOICE_S
WHERE
MONTH_ID = '201801'
GROUP BY
MONTH_ID,user_id;



INSERT INTO MID_M_CB_VOICE_S

SELECT
MONTH_ID,user_id,
sum(CASE WHEN TOTAL_FEE > 0 THEN CALL_DURATION
ELSE 0 END) / 60 AS VOICE_OUT,
SUM(CDR_NUMS) AS CALL_TIMES,
sum(CASE WHEN CALL_TYPE = '01' THEN CDR_NUMS
ELSE 0 END) AS CALLING_TIMES
FROM
DWA.DWA_S_M_USE_CB_VOICE_S
WHERE
MONTH_ID = '201801'
GROUP BY
MONTH_ID,user_id;



--中间表 产生话单天数
INSERT INTO MID_M_MB_NO_VOICE

select MONTH_ID,USER_ID,count(DISTINCT DAY_ID) AS NO_VOICE_DAYS from 
dwd.DWD_D_USE_MB_VOICE
where month_id = '201801'
GROUP BY MONTH_ID,USER_ID;



INSERT INTO MID_M_CB_NO_VOICE

select MONTH_ID,USER_ID,count(DISTINCT DAY_ID) AS NO_VOICE_DAYS from
dwd.DWD_D_USE_CB_VOICE
where month_id = '201801'
GROUP BY MONTH_ID,USER_ID;



--中间表 每日是否出现在机场
INSERT INTO MID_D_MB_AIRPORT_DAYS

select USER_ID,count(*) as is_airport from 
(select month_id,user_id,day_id,cell_id,lac_id 
from dwa.DWA_S_D_USE_MB_BS where month_id ='201801' AND DAY_ID = '22') a 
left outer join dwd.DWD_D_RES_AL_BS_INFO b 
on a.cell_id = b.cell_id and a.lac_id = b.lac_id 
where b.cell_name like '%机场%'
GROUP BY USER_ID;


INSERT INTO MID_D_CB_AIRPORT_DAYS

select USER_ID,count(*) as is_airport from 
(select month_id,user_id,day_id,cell_id,lac 
from dwd.DWD_D_USE_CB_FLUX where month_id ='201801'and day_id='22') a 
left outer join dwd.DWD_D_RES_AL_BS_INFO b 
on a.cell_id = b.cell_id and a.lac = b.lac_id
where b.cell_name like '%机场%'
GROUP BY USER_ID;




--BSS_D
INSERT INTO table 
DWA_A_D_PRO_MB_USER 

SELECT
'201801' MONTH_ID,
'22' DAY_ID,
A.USER_ID,
A.DEVICE_NUMBER,
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX, 0) AS FLUX_PRO_USED_D, --套餐内使用流量
NVL(B.UP_ROAM_PROV_FLUX + B.DOWN_ROAM_PROV_FLUX, 0) as ROAM_PROV_FLUX_D, --省内漫游流量
NVL(B.FLUX_4G, 0) as FLUX_4G_d, --4G基站流量
NVL(B.DOWN_ROAM_INT_FLUX + B.UP_ROAM_INT_FLUX, 0) AS ROAM_INT_FLUX_D, --国际漫游流量
NVL(B.UP_LOCAL_FLUX + B.DOWN_LOCAL_FLUX, 0) AS LOCAL_FLUX_D,  --本地使用流量
NVL(B.DOWN_ROAM_CONT_FLUX + B.UP_ROAM_CONT_FLUX, 0) AS ROAM_COUNT_FLUX_D, --国内漫游使用流量
NVL(B.BILL_FLUX, 0) AS BILL_FLUX_D, --超出主套餐的流量
NVL(A1.SMS_OUT, 0) AS SMS_OUT_D, --套餐外短信
NVL(C.BZ_FEE_FLUX, 0) AS BZ_FEE_FLUX_D, --标准资费流量
NVL(A1.SEND_SMS_NUM, 0) AS SEND_SMS_NUM_D, --发送短信数
'0' voice_out_d, 
NVL(E.SEND_MMS_NUM, 0) AS SEND_MMS_NUM_D, --发送彩信数
NVL(F.ROAM_BS_NUM, 0) AS ROAM_BS_NUM_D, --漫游基站数
NVL(I.ACTIVE_BS_NUM, 0) AS ACTIVE_BS_NUM_D,--使用活跃基站数
NVL(K.xianshi_flux, 0) AS xianshi_flux_D, --闲时流量使用量
NVL(L.FREE_LIMIT_REMAIN, 0) AS FREE_LIMIT_REMAIN_D, --可用流量
NVL(M.CALL_10010, 0) AS CALL_10010_D, --拨打客服次数
NVL(Q.free_limit_used,0) AS DX_FLUX_USED_D,--定向流量使用量
CASE 
WHEN NVL(O.roam_bs_num,0) > 0 THEN 1
ELSE 0 END AS IS_ROAM_TODAY, --是否漫游
CASE 
WHEN NVL(P.is_airport, 0) > 0 THEN 1
ELSE 0 END AS is_airport --是否出现在机场
FROM
(SELECT USER_ID,MONTH_ID,DAY_ID,DEVICE_NUMBER FROM DWD.DWD_D_PRD_MB_NET_USER_INFO
WHERE MONTH_ID = '201801' AND DAY_ID = '22' AND STATUS NOT IN ('41','42')) A
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,user_id,sum(CASE WHEN TOTAL_FEE > 0 AND SMS_DIRECT = '01' THEN CDR_NUM 
ELSE 0 END) AS SMS_OUT,
SUM(CASE WHEN SMS_DIRECT = '01' THEN CDR_NUM ELSE 0 END) AS SEND_SMS_NUM 
from dwa.DWA_S_D_USE_MB_SMS where day_id='22' and month_id='201801' GROUP BY MONTH_ID,DAY_ID,user_id ) A1
ON
A.USER_ID = A1.USER_ID
LEFT OUTER JOIN
(SELECT * FROM DWA.DWA_V_D_CUS_MB_SING_FLUX
WHERE MONTH_ID ='201801' AND DAY_ID = '22') B
ON
A.USER_ID = B.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,USER_ID,sum(total_bytes) AS BZ_FEE_FLUX from dwa.DWA_S_D_USE_MB_FLUX where stream_type=1 and month_id='201801' and day_id='22'
GROUP BY MONTH_ID,DAY_ID,user_id) C
ON
A.USER_ID = C.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,USER_ID,count(*) AS SEND_MMS_NUM from dwd.DWD_D_USE_MB_MMS where SMS_DIRECT='01' and month_id='201801' and day_id='22'
 GROUP BY MONTH_ID,DAY_ID,user_id) E
ON
 A.USER_ID = E.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,USER_ID,count(distinct lac_id, cell_id) AS ROAM_BS_NUM from dwa.DWA_S_D_USE_MB_BS where roam_type <> '01AA' AND DAY_ID='22' and month_id='201801'
 GROUP BY MONTH_ID,user_id) F
ON
A.MONTH_ID = F.MONTH_ID
AND A.USER_ID = F.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,USER_ID,count(*) as ACTIVE_BS_NUM from 
(select MONTH_ID,DAY_ID,lac_id,cell_id,USER_ID,sum(FLUX_UP + FLUX_DOWN) AS FLUX,sum(call_times) AS CALL_TIMES from dwa.DWA_S_D_USE_MB_BS where month_id='201801' and day_id='22'
group by MONTH_ID,DAY_ID,user_id,lac_id,cell_id) T_B
where FLUX >= 1024 AND CALL_TIMES > 0
group by MONTH_ID,DAY_ID,user_id) I
ON
 A.USER_ID = I.USER_id
LEFT OUTER JOIN
(SELECT sum(TOTAL_BYTES) as xianshi_flux,USER_ID FROM dwa.DWA_S_D_USE_MB_FLUX WHERE (HOUR_SEG between '00' and '08') and month_id='201801' and day_id='22'
group by user_id) K
ON
A.USER_ID = K.USER_ID
LEFT OUTER JOIN
(select user_id,sum(FREE_LIMIT_REMAIN) as FREE_LIMIT_REMAIN from DWA.DWA_S_D_USE_MB_PKG_GPRS_USED where month_id='201801' and day_id='22' group by user_id) L
ON
A.user_id= L.user_id
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,user_id,count(*) AS CALL_10010 from dwd.DWD_D_USE_MB_VOICE where OPPOSE_NUMBER = '10010' and month_id='201801' and day_id='22'
GROUP BY MONTH_ID,user_id,DAY_ID) M
ON
A.MONTH_ID = M.MONTH_ID
AND A.user_id = M.user_id
AND A.DAY_ID = M.DAY_ID
LEFT OUTER JOIN
(select user_id,count(*) as roam_bs_num 
from dwa.DWA_S_D_USE_MB_BS where roam_type <> '01AA' and month_id = '201801' 
and day_id = '22'
GROUP BY USER_ID)  O
ON
A.user_id = O.user_id
LEFT OUTER JOIN
MID_D_MB_AIRPORT_DAYS P
ON
A.USER_ID = P.USER_ID
LEFT OUTER JOIN
(SELECT user_id,SUM(free_limit_used) AS free_limit_used from dwd.dwd_d_use_mb_pkg_gprs_used
where month_id = '201801' and day_id = '22' and sub_flowtype like 'L06%'
GROUP BY USER_ID) Q
ON
A.USER_ID = Q.USER_ID;



--CB_D
INSERT INTO
DWA_A_D_PRO_CB_USER


SELECT
A.MONTH_ID,
A.DAY_ID,
A.USER_ID,
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX,0) AS FLUX_PRO_USED_D, --套餐内使用流量
NVL(B.UP_ROAM_PROV_FLUX + B.DOWN_ROAM_PROV_FLUX,0) as ROAM_PROV_FLUX_D, --省内漫游流量
NVL(B.FLUX_4G,0) as FLUX_4G_d, --4G基站流量
NVL(B.DOWN_ROAM_INT_FLUX + B.UP_ROAM_INT_FLUX,0) AS ROAM_INT_FLUX_D, --国际漫游流量
NVL(B.UP_LOCAL_FLUX + B.DOWN_LOCAL_FLUX,0) AS LOCAL_FLUX_D,  --本地使用流量
NVL(B.UP_ROAM_CONT_FLUX + B.DOWN_ROAM_CONT_FLUX, 0) AS ROAM_COUNT_FLUX_D, --国内漫游使用流量
NVL(B.BILL_FLUX,0) AS BILL_FLUX_D, --超出主套餐的流量
NVL(A1.SMS_OUT,0) AS SMS_OUT_D,  --套餐外短信
NVL(C.BZ_FEE_FLUX,0) AS BZ_FEE_FLUX_D,  --标准资费流量
NVL(A1.SEND_SMS_NUM,0) AS SEND_SMS_NUM_D, --发送短信数
NVL(D.VOICE_OUT,0) AS VOICE_OUT_D,  --使用套外语音分钟数
'0' SEND_MMS_NUM_D, --发送彩信数
NVL(E.roam_bs_num,0) AS roam_bs_num_D,  --漫游基站数
NVL(F.ACTIVE_BS_NUM,0) AS ACTIVE_BS_NUM_D,  --活跃基站数
'0' DX_FLUX_USED_D, --定向流量使用量
NVL(G.xianshi_flux,0) AS xianshi_flux_D,  --闲时流量使用量
'0' FREE_LIMIT_REMAIN_D,  --可用流量
NVL(I.CALL_10010,0) AS CALL_10010_D,  --拨打客服次数
CASE
WHEN NVL(J.roam_bs_num, 0) > 0 THEN 1
ELSE 0 END AS IS_ROAM_TODAY,  --是否漫游
CASE
WHEN NVL(K.is_airport,0) > 0 THEN 1
ELSE 0 END AS is_airport  --是否出现在机场
FROM
(SELECT USER_ID,MONTH_ID,DAY_ID FROM ODS.ODS_D_PRD_CB_USER
WHERE MONTH_ID = '201801' AND DAY_ID = '22') A
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,USER_ID,sum(CASE WHEN TOTAL_FEE > 0 AND SMS_DIRECT = '01' THEN CDR_NUM 
ELSE 0 END) AS SMS_OUT,
SUM(CASE WHEN SMS_DIRECT = '01' THEN CDR_NUM ELSE 0 END) AS SEND_SMS_NUM 
from dwa.DWA_S_D_USE_CB_SMS where month_id='201801' and day_id='22' GROUP BY MONTH_ID,DAY_ID,user_id) A1
ON
A.USER_ID = A1.USER_ID
LEFT OUTER JOIN
(SELECT * FROM DWA.DWA_V_D_CUS_CB_SING_FLUX
WHERE MONTH_ID = '201801' AND DAY_ID = '22') B
ON
A.USER_ID = B.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,user_id,sum(total_bytes) AS BZ_FEE_FLUX from dwa.DWA_S_D_USE_CB_FLUX where TOTAL_FEE=BEF_BASE_FEE and month_id='201801' and day_id='22'
 GROUP BY MONTH_ID,DAY_ID,user_id) C
ON
A.USER_ID = C.user_id
AND A.MONTH_ID = C.MONTH_ID
AND A.DAY_ID = C.DAY_ID
LEFT OUTER JOIN
(select MONTH_ID,DAY_ID,USER_ID,sum(CALL_DURATION) / 60 AS VOICE_OUT from dwa.DWA_S_D_USE_CB_VOICE_S where total_fee > 0  and month_id='201801' and day_id='22'
GROUP BY MONTH_ID,DAY_ID,user_id) D
ON
A.USER_ID = D.USER_ID
AND A.MONTH_ID = D.MONTH_ID
AND A.DAY_ID = D.DAY_ID
LEFT OUTER JOIN
(select MONTH_ID,USER_ID,count(distinct lac, cell_id) AS roam_bs_num from dwd.DWD_D_USE_CB_FLUX where day_id='22' and month_id='201801' and roam_type_cbss <> '0' 
            GROUP BY MONTH_ID, user_id) E
ON
A.USER_ID = E.USER_ID
LEFT OUTER JOIN
(select USER_ID,count(*) as ACTIVE_BS_NUM from 
  (select MONTH_ID,DAY_ID,lac,cell_id,USER_ID,sum(FLUX_UP + FLUX_DOWN) AS FLUX,sum(case when CALL_DURATION > 0 then 1 else 0 end) > 0 AS CALL_TIMES from 
             dwd.DWD_D_USE_CB_FLUX where month_id='201801' and day_id='22' group by MONTH_ID,DAY_ID,user_id,lac,cell_id) a where A.FLUX >= 1024 OR A.CALL_TIMES > 0 
             GROUP BY user_id) F
ON
A.USER_ID = F.USER_ID
LEFT OUTER JOIN
(select USER_ID,sum(TOTAL_BYTES) as xianshi_flux from dwa.DWA_S_D_USE_CB_FLUX 
where (HOUR_SEG between '23' and '24') or (HOUR_SEG between '00' and '07') and month_id='201801' and day_id='22' group by user_id) G
ON
A.USER_ID = G.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,USER_ID,count(*) AS CALL_10010 from dwd.DWD_D_USE_CB_VOICE where OPPOSE_NUMBER = '10010' and month_id='201801' and day_id='22'
GROUP BY MONTH_ID,user_id) I
ON
A.USER_ID = I.USER_ID
LEFT OUTER JOIN
(select month_id,user_id,count(*) as roam_bs_num 
  from dwa.DWA_S_D_USE_CB_FLUX where roam_type <> '01AA'
  AND MONTH_ID = '201801' AND DAY_ID ='22' group by month_id,user_id) J
ON
A.user_id = J.user_id
LEFT OUTER JOIN
MID_D_CB_AIRPORT_DAYS K
ON
A.USER_ID = K.USER_ID;



--BSS_M
INSERT INTO
DWA_A_M_PRO_MB_USER

select
A.USER_ID,
A.MONTH_ID, --月份
A.DEVICE_NUMBER,  --电话号码
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX, 0) AS FLUX_PRO_USED, --套餐内使用流量
NVL(B.FLUX_4G, 0) AS FLUX_4G, --4g基站流量
NVL(B.TOTAL_FEE, 0) AS TOTAL_FEE, --超出套餐流量费用
NVL(B.BILL_FLUX, 0) AS BILL_FLUX,--超出主套餐的流量
NVL(A1.SMS_OUT, 0) AS SMS_OUT, --套餐外短信数量
NVL(C.BZ_FEE_FLUX, 0) AS BZ_FEE_FLUX, --标准资费流量
NVL(B.DOWN_ROAM_CONT_FLUX + B.UP_ROAM_CONT_FLUX, 0) AS ROAM_COUNT_FLUX, --国内漫游使用流量
NVL(A1.SEND_SMS_NUM, 0) AS SEND_SMS_NUM,  --发送短信数
NVL(D.VOICE_OUT, 0) AS VOICE_OUT, --使用套外语音分钟数
NVL(D.CALL_TIMES, 0) AS CALL_TIMES, --通话次数
NVL(D.CALLING_TIMES, 0) AS CALLING_TIMES, --主叫通话时长
--NVL(G.lac_id, '--') AS lac_id,  --常住基站lac
--NVL(G.cell_id, '--') AS cell_id,  --常住基站cell_id
NVL(J.jiaowang_nums, 0) AS jiaowang_nums, --交往圈数
DAY(date_add('2018-02-01',-1)) - NVL(K.NO_VOICE_DAYS, 0) AS NO_VOICE_DAYS, --未产生话单天数
NVL(N.xianshi_flux, 0) AS xianshi_flux,--闲时流量使用量
NVL(PRO_D.DX_FLUX_USED, 0) AS DX_FLUX_USED,--定向流量使用量
NVL(PRO_D.CALL_10010,0) AS CALL_10010,--拨打客服次数
NVL(PRO_D.SEND_MMS_NUM,0) AS SEND_MMS_NUM,--发送彩信数
NVL(PRO_D.roam_day,0) AS roam_day,--漫游天数
NVL(PRO_D.AIRPORT_DAYS,0) AS AIRPORT_DAYS--出现在机场天数
FROM 
(SELECT USER_ID,DEVICE_NUMBER,MONTH_ID FROM DWD.DWD_M_PRD_MB_NET_USER_INFO
WHERE MONTH_ID = '201801' AND STATUS NOT IN ('41','42')) A
LEFT OUTER JOIN
(select MONTH_ID,user_id,sum(CASE WHEN TOTAL_FEE > 0 AND SMS_DIRECT = '01' THEN CDR_NUM 
ELSE 0 END) AS SMS_OUT,
SUM(CASE WHEN SMS_DIRECT = '01' THEN CDR_NUM ELSE 0 END) AS SEND_SMS_NUM
from dwa.DWA_S_M_USE_MB_SMS where month_id='201801' GROUP BY MONTH_ID,user_id) A1
ON
A.USER_ID = A1.USER_ID
LEFT OUTER JOIN
(SELECT * FROM DWA.DWA_V_M_CUS_MB_SING_FLUX
WHERE MONTH_ID = '201801') B
ON
A.user_id = B.user_id
AND A.MONTH_ID = B.MONTH_ID
LEFT OUTER JOIN
(select MONTH_ID,user_id,sum(total_bytes) AS BZ_FEE_FLUX from dwa.DWA_S_M_USE_MB_FLUX where stream_type=1 and month_id='201801' GROUP BY MONTH_ID,user_id) C
ON
A.user_id = C.user_id
AND A.MONTH_ID = C.MONTH_ID
LEFT OUTER JOIN
MID_M_MB_VOICE_S D
ON
A.user_id = D.user_id
-----LEFT OUTER JOIN
-----of_dwa.dwa_v_m_bs_place G
------ON
-----A.MONTH_ID = G.MONTH_ID
-----AND A.DEVICE_NUMBER = G.DEVICE_NUMBER
LEFT OUTER JOIN
(select t_c.MONTH_ID,t_c.user_id,count(*) as jiaowang_nums from 
(select MONTH_ID,user_id,OPPOSE_NUMBER,count(*) as oppo_call_cnt 
from dwd.DWD_D_USE_MB_VOICE where month_id='201801'
group by OPPOSE_NUMBER,MONTH_ID,user_id) T_C
where t_c.oppo_call_cnt >= 3
group by t_c.month_id,t_c.user_id) J
ON
A.MONTH_ID = J.MONTH_ID
AND A.user_id = J.user_id
LEFT OUTER JOIN
MID_M_MB_NO_VOICE K
ON
A.user_id = K.user_id
LEFT OUTER JOIN
(SELECT sum(TOTAL_BYTES) as xianshi_flux,user_id FROM dwa.DWA_S_M_USE_MB_FLUX WHERE (HOUR_SEG between '00' and '08') and month_id='201801' group by user_id) N
ON
A.user_id = N.user_id
LEFT OUTER JOIN
(SELECT 
USER_ID,MONTH_ID,
MAX(DX_FLUX_USED_D) AS DX_FLUX_USED,--定向流量使用量
SUM(CALL_10010_D) AS CALL_10010,--拨打客服次数
SUM(SEND_MMS_NUM_D) AS SEND_MMS_NUM,--发送彩信数
SUM(IS_ROAM_TODAY) AS roam_day,--漫游天数
SUM(is_airport) AS AIRPORT_DAYS--出现在机场天数
FROM
DWA_A_D_PRO_MB_USER
GROUP BY USER_ID,MONTH_ID) PRO_D
ON
A.USER_ID = PRO_D.USER_ID
AND A.MONTH_ID = PRO_D.MONTH_ID;



--CB_M
INSERT INTO
DWA_A_M_PRO_CB_USER

SELECT
A.USER_ID,
A.MONTH_ID, --月份
NVL(A1.DEVICE_NUMBER, '--') AS DEVICE_NUMBER,
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX,0) AS FLUX_PRO_USED, --套餐内使用流量
NVL(B.FLUX_4G,0),  --4g基站流量
NVL(B.DOWN_ROAM_CONT_FLUX + B.UP_ROAM_CONT_FLUX,0) AS ROAM_COUNT_FLUX, --国内漫游使用流量
NVL(B.TOTAL_FEE,0) AS TOTAL_FEE, --超出套餐流量费用
NVL(B.BILL_FLUX,0) AS BILL_FLUX, --超出主套餐的流量
NVL(A1.SMS_OUT,0) AS SMS_OUT,  --套餐外短信数量
NVL(A1.SEND_SMS_NUM,0) AS SEND_SMS_NUM, --发送短信数
NVL(D.VOICE_OUT,0) AS VOICE_OUT,  --使用套外语音分钟数
NVL(D.CALL_TIMES,0) AS CALL_TIMES,  --通话次数
NVL(D.CALLING_TIMES,0) AS CALLING_TIMES,  --主叫通话时长
'0' SEND_MMS_NUM, --发送彩信数
-----F.lac_id,  --常驻基站lac
------F.cell_id,  --常住基站cell_id
NVL(I.jiaowang_nums,0) AS jiaowang_nums,  --交往圈数
DAY(date_add('2018-02-01',-1)) - NVL(J.NO_VOICE_DAYS, 0) AS NO_VOICE_DAYS,  --未产生话单天数
NVL(H.ACTIVE_BS_NUM,0) AS ACTIVE_BS_NUM,  --使用活跃基站数
'0' DX_FLUX_USED, --定向流量使用量
NVL(K.xianshi_flux,0) AS xianshi_flux,  --闲时流量使用量
NVL(PRO_D.BZ_FEE_FLUX,0) AS BZ_FEE_FLUX,--标准资费流量
NVL(PRO_D.CALL_10010,0) AS CALL_10010,--拨打客服次数
NVL(PRO_D.roam_day,0) AS roam_day,--漫游天数
NVL(PRO_D.AIRPORT_DAYS,0) AS AIRPORT_DAYS--出现在机场天数
FROM
(SELECT USER_ID,MONTH_ID FROM DWA.DWA_V_M_CUS_CB_USER_INFO
WHERE MONTH_ID = '201801') A
LEFT OUTER JOIN
(select MONTH_ID,DEVICE_NUMBER,USER_ID,sum(CASE WHEN TOTAL_FEE > 0 AND SMS_DIRECT = '01' THEN CDR_NUM 
ELSE 0 END) AS SMS_OUT,
SUM(CASE WHEN SMS_DIRECT = '01' THEN CDR_NUM ELSE 0 END) AS SEND_SMS_NUM 
from dwa.DWA_S_M_USE_CB_SMS where month_id='201801' GROUP BY MONTH_ID,DEVICE_NUMBER,user_id) A1
ON
A.USER_ID = A1.USER_ID
LEFT OUTER JOIN
(SELECT * FROM DWA.DWA_V_M_CUS_CB_SING_FLUX
WHERE MONTH_ID = '201801') B
ON
A.USER_ID = B.USER_ID
AND A.MONTH_ID = B.MONTH_ID
LEFT OUTER JOIN
MID_M_CB_VOICE_S D
ON
A.USER_ID = D.USER_ID
------LEFT OUTER JOIN
-----of_dwa.dwa_v_m_bs_place F
-----ON
-----A.MONTH_ID = F.MONTH_ID
-------AND A.DEVICE_NUMBER = F.DEVICE_NUMBER
LEFT OUTER JOIN
(select MONTH_ID,USER_ID,count(*) as ACTIVE_BS_NUM from 
   (select MONTH_ID,USER_ID,lac,cell_id,sum(FLUX_UP + FLUX_DOWN) AS FLUX,sum(case when CALL_DURATION > 0 then 1 else 0 end) > 0 AS CALL_TIMES
              from dwd.DWD_D_USE_CB_FLUX where month_id='201801' group by MONTH_ID,user_id,lac,cell_id) w where w.FLUX >= 1024 OR w.CALL_TIMES > 0 GROUP BY MONTH_ID,user_id) H
ON
A.MONTH_ID = H.MONTH_ID
AND A.USER_ID = H.USER_ID
LEFT OUTER JOIN
(select MONTH_ID,USER_ID,count(*) as jiaowang_nums from 
    (select MONTH_ID,user_id,OPPOSE_NUMBER,count(*) as oppo_call_cnt 
      from dwd.DWD_D_USE_MB_VOICE where month_id='201801'
      group by OPPOSE_NUMBER,MONTH_ID,user_id) T_C
      where oppo_call_cnt >= 3 GROUP BY MONTH_ID,user_id) I
ON
A.MONTH_ID = I.MONTH_ID
AND A.USER_ID = I.USER_ID
LEFT OUTER JOIN
MID_M_CB_NO_VOICE J
ON
A.USER_ID = J.USER_ID
AND J.MONTH_ID = A.MONTH_ID
LEFT OUTER JOIN
(select sum(TOTAL_BYTES) as xianshi_flux,user_id,month_id from dwa.DWA_S_M_USE_CB_FLUX 
where (HOUR_SEG between '23' and '24') or (HOUR_SEG between '00' and '07') group by user_id,month_id) K
on A.MONTH_ID = K.MONTH_ID
AND A.USER_ID = K.USER_ID
LEFT OUTER JOIN
(SELECT
USER_ID,MONTH_ID,
SUM(BZ_FEE_FLUX_D) AS BZ_FEE_FLUX,
SUM(CALL_10010_D) AS CALL_10010,
SUM(IS_ROAM_TODAY) AS roam_day,
SUM(is_airport) AS AIRPORT_DAYS
FROM
DWA_A_D_PRO_CB_USER
GROUP BY USER_ID,MONTH_ID) PRO_D
ON
A.USER_ID = PRO_D.USER_ID
AND A.MONTH_ID = PRO_D.MONTH_ID;




-----余华清低消
----BSS-D
insert into table mid_d_mb_dixiao
SELECT
    A.USER_ID AS USER_ID,
    '0' AS is_ice,--是否办理畅越冰激凌,
    (CASE WHEN T1.MIX_FLAG = '2' THEN '1' ELSE '0' END) AS is_flux_pkg,--是否订购流量包,
'' AS FLUX_CHANNEL_TYPE,  --流量包受理渠道,
'' AS FLUX_CHANNEL_ID,--流量包受理渠道编码,
    '' AS ICE_PRODUCT_ID,--畅越冰激凌产品代码,
    '' AS ICE_ACCEPT_DATE,--畅越冰激凌受理时间,
    '' AS ICE_TRADE_STAFF_ID,--越冰激凌受理工号,
    '' AS ICE_TRADE_DEPART_ID,--畅越冰激凌受理渠道编码,
    '' AS ICE_PRODUCT_NAME,--畅越冰激凌名称,
    '' AS ice_dangwei,--畅越冰激凌档位

    (CASE WHEN T2.code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') THEN '1' ELSE '0' END) AS IS_LIUSHI,--是否受理流失预警,
    T2.OPER_NO AS LIUSHI_OPER_NO,--流失预警受理工号,
    T2.mkt_sub_title AS LIUSHI_mkt_sub_title,--受理流失预警业务名称,
    T2.code_mkt AS LIUSHI_code_mkt,--受理流失预警营销代码,
    T3.CHANNEL_ID AS LIUSHI_CHANNEL_ID,--流失预警渠道编码,
    T3.CHANNEL_TYPE AS LIUSHI_CHANNEL_TYPE,--流失预警受理渠道,

    '' HY_TYPE,---------T4.HY_TYPE AS HY_TYPE,--续约的合约类型,
    '' HY_ACCEPT_AREA_NO,-------T4.ACCEPT_AREA_NO AS HY_ACCEPT_AREA_NO,--合约续约受理地市,
    '' HY_oper_no,--------T4.oper_no AS HY_oper_no,--合约续约受理工号,
    '' HY_CHNL_NAME,------T4.CHNL_NAME AS HY_CHNL_NAME,--合续约受理渠道,
    '' HY_CHNL_CODE,------T4.CHNL_CODE AS HY_CHNL_CODE,--合约续约渠道编码,
    '' is_auto_renew,------T4.is_auto_renew AS is_auto_renew, --是否自动续约
    T5.OPER_DATE AS DIXIAO_ACCEPT_DATE,--低消受理时间,
    T5.OPER_NO AS DIXIAO_TRADE_STAFF_ID,--低消受理工号,
    T5.CODE_MKT AS DIXIAO_DISCNT_CODE,--受理低消业务代码,
    T5.MKT_SUB_TITLE AS dixiao_product_name,--受理低消业务名称,
    (CASE WHEN T5.CODE_MKT IS NOT NULL THEN 1 ELSE 0 end) AS IS_ACCEPT_DIXIAO, --是否办理低消业务,
    T6.CHANNEL_ID AS DIXIAO_TRADE_DEPART_ID,--低消受理渠道编码,
    T6.CHANNEL_TYPE AS DIXIAO_CHANNEL_TYPE--低消受理渠道名称,
 
FROM (SELECT *
 FROM DWA.DWA_V_D_CUS_MB_USER_DERIVE T
 WHERE T.MONTH_ID = '201801' 
 AND T.DAY_ID = '22'
 AND T.IS_INNET = '1') A
 
    LEFT OUTER JOIN 
(SELECT USER_ID,PRO_FLAG,MIX_FLAG 
FROM DWA.DWA_V_D_CUS_MB_MIX_OPEN 
WHERE MONTH_ID = '201801' 
AND DAY_ID = '22' 
AND MIX_FLAG ='2') T1 
ON A.USER_ID=T1.USER_ID
 
    LEFT OUTER JOIN 
(select user_id,code_mkt,mkt_sub_title,OPER_NO 
from DWD.DWD_D_EVT_MB_MKT_ACTIVITY 
where code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') 
AND MONTH_ID = '201801' 
AND DAY_ID = '22') T2 
ON A.USER_ID=T2.USER_ID
 
    LEFT OUTER JOIN 
(SELECT user_id,CODE_MKT,MKT_SUB_TITLE,CHANNEL_ID,CHANNEL_TYPE 
FROM DWD.DWD_D_PRD_MB_MKT_INFO 
WHERE CODE_MKT IN ('MKT108701','MKT108702','MKT108703','MKT108704') 
AND MONTH_ID = '201801' 
AND DAY_ID = '22') T3
ON A.USER_ID = T3.USER_ID
  
    LEFT OUTER JOIN 
(SELECT A.USER_ID,A.OPER_DATE,A.OPER_NO,B.CODE_MKT,B.MKT_SUB_TITLE 
  FROM 
    (SELECT * FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY 
     WHERE MONTH_ID='201801' AND DAY_ID='22') A
  RIGHT OUTER JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT) T5 
ON A.USER_ID=T5.USER_ID
    
    LEFT OUTER JOIN 
 (SELECT A.USER_ID,A.CHANNEL_ID,A.CHANNEL_TYPE,B.CODE_MKT,B.MKT_SUB_TITLE 
  FROM 
  (SELECT * 
   FROM DWD.DWD_D_PRD_MB_MKT_INFO 
   WHERE MONTH_ID='201801' AND DAY_ID='22') A 
  RIGHT OUTER JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT) T6 
ON A.USER_ID=T6.USER_ID

---CB-D

insert into table mid_d_cb_dixiao1

SELECT 
A.USER_ID AS USER_ID,
'' is_ice, -------(CASE WHEN T1.PRODUCT_ID IS NOT NULL THEN '1' ELSE '0' END) AS is_ice,--是否办理畅越冰激凌,
(CASE WHEN T2.FLUX_PKG IS NOT NULL THEN '1' ELSE '0' END) AS is_flux_pkg, --是否订购流量包,
'' FLUX_CHANNEL_TYPE, ----T3.CHANNEL_TYPE AS FLUX_CHANNEL_TYPE,--流量包受理渠道,
'' FLUX_CHANNEL_ID,---------T3.CHANNEL_ID AS FLUX_CHANNEL_ID,--流量包受理渠道编码,
T4.PRODUCT_ID AS ICE_PRODUCT_ID,--畅越冰激凌产品代码,
T4.ACCEPT_DATE AS ICE_ACCEPT_DATE,--畅越冰激凌受理时间,
T4.TRADE_STAFF_ID AS ICE_TRADE_STAFF_ID,--越冰激凌受理工号,
T4.TRADE_DEPART_ID AS ICE_TRADE_DEPART_ID,--畅越冰激凌受理渠道编码,
CASE WHEN T4.PRODUCT_ID IS NOT NULL THEN 1 ELSE 0 end is_cybjl --是否办理畅越冰激凌,
FROM (SELECT *
FROM DWA.DWA_V_D_CUS_CB_USER_INFO T
WHERE T.MONTH_ID = '201801' 
AND T.DAY_ID = '22'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN 
(SELECT * 
FROM DWD.DWD_D_PRD_CB_PRODUCT 
WHERE MONTH_ID = '201801' 
AND DAY_ID = '22' 
AND PRODUCT_ID IN ('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')  AND PRODUCT_NAME LIKE '%畅越冰激凌%') T1 
ON A.PRODUCT_ID = T1.PRODUCT_ID --畅越冰激凌
 LEFT OUTER JOIN 
(SELECT USER_ID,FLUX_PKG 
FROM DWA.DWA_V_D_CUS_CB_DIY_PACKAGE --套餐DIY叠加包
WHERE MONTH_ID = '201801' 
AND DAY_ID = '22' 
AND FLUX_PKG IS NOT NULL) T2 --流量叠加包的用户 
ON A.USER_ID=T2.USER_ID
----LEFT OUTER JOIN 
---------(SELECT B.USER_ID,B.FLUX_PKG,C.CHANNEL_ID,C.CHANNEL_TYPE 
--------FROM (select * 
------from DWA.DWA_V_D_CUS_CB_DIY_PACKAGE 
-------WHERE MONTH_ID = '201801' 
-------AND DAY_ID = '22' 
--------AND FLUX_PKG IS NOT NULL) B
--------LEFT OUTER JOIN (SELECT USER_ID,PACKAGE_ID,CHANNEL_ID,CHANNEL_TYPE
-------FROM DWA.DWA_V_D_CUS_CB_DISCNT_INFO
-------WHERE MONTH_ID = '201801'
---------AND DAY_ID = '22') C
--------ON B.PACKAGE_ID=C.PACKAGE_ID) T3  --流量包订购渠道
--------ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN
  (SELECT * FROM DWD.DWD_D_EVT_CB_TRADE_PRODUCT--cBSS业务台账产品子表
 WHERE MONTH_ID = '201801'  AND DAY_ID = '22' AND PRODUCT_ID IN('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) T4
ON A.USER_ID=T4.USER_ID
where A.month_id='201801' and a.day_id='22'



insert into table mid_d_cb_dixiao2

select
a.user_id,
T5.PRODUCT_NAME AS ICE_PRODUCT_NAME,--畅越冰激凌名称,
(substr(T5.PRODUCT_NAME, instr(T5.PRODUCT_NAME, '-') + 1, instr(T5.PRODUCT_NAME, '元') - instr(T5.PRODUCT_NAME, '-') - 1)) AS ice_dangwei,--畅越冰激凌档位
'' AS IS_LIUSHI,--是否受理流失预警,
'' AS LIUSHI_OPER_NO, --流失预警受理工号,
'' AS LIUSHI_mkt_sub_title,--受理流失预警业务名称,
'' AS LIUSHI_code_mkt, --受理流失预警营销代码,
'' AS LIUSHI_CHANNEL_ID, --流失预警渠道编码,
'' AS LIUSHI_CHANNEL_TYPE, --流失预警受理渠道,
'' HY_TYPE,--------T6.HY_TYPE AS HY_TYPE, --续约的合约类型, 
'' HY_ACCEPT_AREA_NO,-----T6.ACCEPT_AREA_NO AS HY_ACCEPT_AREA_NO,--合约续约受理地市,
'' HY_oper_no,------T6.oper_no AS HY_oper_no,--合约续约受理工号,
'' HY_CHNL_NAME,-----T6.CHNL_NAME AS HY_CHNL_NAME,--合约续约受理渠道,
'' HY_CHNL_CODE,-------T6.CHNL_CODE AS HY_CHNL_CODE,--合约续约渠道编码,
'' is_auto_renew,------T6.is_auto_renew AS is_auto_renew,--是否自动续约
T7.ACCEPT_DATE AS DIXIAO_ACCEPT_DATE,--低消受理时间,
T7.TRADE_STAFF_ID AS DIXIAO_TRADE_STAFF_ID,--低消受理工号,
T7.DISCNT_CODE AS DIXIAO_DISCNT_CODE,--受理低消营销代码,
(CONCAT(T7.MKT_SUB_TITLE + T7.DISCNT_NAME)) AS dixiao_product_name,--受理低消业务名称,
(CASE WHEN T7.DISCNT_CODE IS NOT NULL THEN 1 ELSE 0 end) AS IS_ACCEPT_DIXIAO, --是否办理低消业务,
T7.TRADE_DEPART_ID AS DIXIAO_TRADE_DEPART_ID,--低消受理渠道编码,
'' AS DIXIAO_CHANNEL_TYPE--低消受理渠道名称
FROM (SELECT user_id
FROM DWA.DWA_V_D_CUS_CB_USER_INFO T
WHERE T.MONTH_ID = '201801' 
AND T.DAY_ID = '22'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN 
(SELECT A.USER_ID,A.PRODUCT_ID,B.PRODUCT_NAME FROM
   (SELECT * FROM DWD.DWD_D_EVT_CB_TRADE_PRODUCT WHERE MONTH_ID = '201803'  AND DAY_ID = '07' AND PRODUCT_ID IN ('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) A
        LEFT OUTER JOIN
        (SELECT PRODUCT_ID,PRODUCT_NAME FROM DWD.DWD_D_PRD_CB_PRODUCT WHERE MONTH_ID = '201803' AND DAY_ID = '07') B 
         ON A.PRODUCT_ID=B.PRODUCT_ID) T5
ON A.USER_ID=T5.USER_ID
LEFT OUTER JOIN 
(SELECT A.ID AS user_id,A.ACCEPT_DATE,A.TRADE_DEPART_ID,A.TRADE_STAFF_ID,B.DISCNT_CODE,B.MKT_SUB_TITLE,B.DISCNT_NAME 
   FROM 
   (SELECT * FROM DWD.DWD_D_EVT_CB_TRADE_DISCNT WHERE MONTH_ID='201801' AND DAY_ID='22') A 
     RIGHT OUTER JOIN DIXIAO_CB_MKT B 
     ON A.PRODUCT_ID=B.PRODUCT_ID) T7 
ON A.USER_ID=T7.user_id

-----低销CB日合并
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
on a.user_id=b.user_id



--CB-M
insert into table mid_m_cb_dixiao

SELECT A.USER_ID,
    '' PURCH_MODE_TYPE_DESC,
  CONCAT(T2.MKT_SUB_TITLE + T2.DISCNT_NAME) as dixiao_type, --低消活动类型,
  CASE WHEN T2.DISCNT_NAME IS NOT NULL THEN '1' ELSE '0' end is_promise_dixiao,--是否承诺低消系列用户
  CASE WHEN T3.DISCNT_CODE IS NOT NULL THEN '1' ELSE '0' end is_participate_dixiao, --是否有参加营销活动,
  T3.PRODUCT_NAME as yingxiao_product_name, --营销活动名称,
  CASE WHEN T4.FLUX_PKG_01 IS NOT NULL THEN '1' ELSE '0' end is_pkg_user, --是否流量包用户,
  CASE  WHEN T5.OPER_DATE = '20180122' AND (T5.PURCH_MODE_CODE IS NOT NULL OR T5.LOG_USER_MKT_SN IS NOT NULL) THEN '1' ELSE '0' end is_auto --是否自动续约
  FROM (SELECT *
            FROM DWA.DWA_V_M_CUS_CB_USER_INFO T
   WHERE T.MONTH_ID = '201801'
             AND T.IS_INNET = '1'
             AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN (select USER_ID,PRODUCT_ID, PACKAGE_ID,DISCNT_CODE from dwd.DWD_D_PRD_CB_USER_DISCNT WHERE MONTH_ID='201801' AND DAY_ID = '22') A
inner join lt_lizhenlin.DIXIAO_CB_MKT B 
on A.PRODUCT_ID=B.PRODUCT_ID and a.PACKAGE_ID=b.package_id and a.DISCNT_CODE=b.discnt_code
group by USER_ID) T2 --29593138
ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN  (SELECT distinct A.USER_ID,A.PRODUCT_ID, A.PACKAGE_ID,A.DISCNT_CODE,B.DISCNT_NAME FROM
 (select USER_ID,PRODUCT_ID, PACKAGE_ID,DISCNT_CODE from dwd.DWD_D_PRD_CB_USER_DISCNT WHERE MONTH_ID='201801' AND DAY_ID = '22' and end_date>'20180314' and
  start_date<'20180314') A 
 LEFT OUTER JOIN (SELECT DISCNT_CODE,DISCNT_NAME FROM zbg_src.SRC_D_BCD07003 WHERE MONTH_ID ='201803' and day_id='14' and discnt_name not 
 like '%开通4G%') B ON A.DISCNT_CODE=B.DISCNT_CODE) T3 
 ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN (SELECT USER_ID,MAX(FLUX_PKG) AS FLUX_PKG_01  FROM DWA.DWA_V_D_CUS_CB_DIY_PACKAGE WHERE MONTH_ID='201801' AND DAY_ID = '22' group by user_id) T4 --DWA_V_D_CUS_CB_DIY_PACKAGE
 on A.USER_ID=T4.USER_ID
LEFT OUTER JOIN (SELECT T1.USER_ID,T5.PURCH_MODE_CODE,T5.PURCH_MODE_NAME,T6.LOG_USER_MKT_SN,T1.OPER_DATE FROM 
                  (SELECT A.USER_ID,
                 CASE
                 WHEN A.ACTIVE_TYPE IN ('A03') THEN
                  '存宽带送手机'
                 WHEN A.ACTIVE_TYPE IN ('06', '07', '08', '09', '10', '11') THEN
                  '存费送业务'
                 WHEN A.ACTIVE_TYPE IN ('A17') THEN
                  'X99'
                 WHEN A.ACTIVE_TYPE IN ('01') THEN
                  '存费送机'
                 WHEN A.ACTIVE_TYPE IN ('A13') THEN
                  '沃易得'
                 WHEN A.ACTIVE_TYPE IN ('03') THEN
                  '存费送费'
                 WHEN A.ACTIVE_TYPE IN ('A14') THEN
                  '集团担保'
                 WHEN A.ACTIVE_TYPE IN ('05') THEN
                  '其他合约'
                 WHEN A.ACTIVE_TYPE IN ('12') THEN
                  '合约惠机'
                 WHEN A.ACTIVE_TYPE IN ('02') THEN
                  '购机送费'
                 ELSE
                  '其他合约'
               END hy_type,
         NULL PURCH_MODE_CODE,
         C.CHANNEL_NO ACCEPT_CHANNEL_NO,
         A.TRADE_STAFF_ID oper_no,
         DATE_TOSTRING(A.ACCEPT_DATE, 'YYYYMMDD') OPER_DATE,
         SUBSTR(A.END_DATE, 1, 8) END_DATE,
         A.RES_IMEI RES_IMEI,
         A.PROD_ID PRODUCT_ID,
         A.TRADE_ID SERVICE_SN,
   D.REGION_ID ACCEPT_AREA_NO
    FROM DWD.DWD_D_PRD_CB_USER_ACTIVE A
    LEFT JOIN DIM.DIM_PUB_ACTIVITY_TYPE B
      ON A.ACTIVE_TYPE = B.ACTIVITY_TYPE
    LEFT JOIN DIM.DIM_PUB_ZB_AGENT_CODE C
      ON A.TRADE_DEPART_ID = c.CHNL_CODE
 LEFT OUTER JOIN DIM.DIM_PUB_OPER_NO D on A.TRADE_STAFF_ID = D.DEPT_NO
   WHERE A.MONTH_ID = '201801'
     AND A.DAY_ID = '22'
     AND A.ACTIVE_TYPE IN ('01', '02', '12')) T1
     LEFT JOIN dm.MID_D_WX_CL_ZDHY_MODE_CODE T5 
      ON T1.PURCH_MODE_CODE = T5.PURCH_MODE_CODE
  LEFT JOIN (SELECT DISTINCT A.LOG_USER_MKT_SN
                 FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY A
                WHERE A.CODE_MKT IN ('MKT032001',
                                   'MKT032002',
                                   'MKT032003',
                                   'MKT032004',
                                   'MKT032005',
                                   'MKT032006')
                  AND A.MKT_LOG_TYPE IN ('10', '20')
                  AND A.MONTH_ID = '201801'
                  AND A.DAY_ID = '22') T6
     ON T1.SERVICE_SN = T6.LOG_USER_MKT_SN) T5
ON A.USER_ID=T5.USER_ID

                  
                  
---bss-M
insert into table mid_m_mb_dixiao


SELECT A.USER_ID,
  T1.PURCH_MODE_TYPE_DESC, --本地合约类型,
  T2.MKT_SUB_TITLE as dixiao_type, --低消活动类型,
  CASE WHEN T2.MKT_SUB_TITLE IS NOT NULL THEN '1' ELSE '0' end is_promise_dixiao, --是否承诺低消系列用户,
  CASE WHEN T3.MKT_SUB_TITLE_01 IS NOT NULL THEN '1' ELSE '0' end is_participate_dixiao, --是否有参加营销活动,
  T3.MKT_ACTIVITY as yingxiao_product_name, --营销活动名称,
  CASE WHEN T4.is_service_mult_01 ='1' THEN '1' ELSE '0' end is_pkg_user, --是否流量包用户,
  CASE  WHEN T5.OPER_DATE = '201801' AND (T5.PURCH_MODE_CODE IS NOT NULL OR T5.LOG_USER_MKT_SN IS NOT NULL) THEN '1 ELSE 0' end is_auto --是否自动续约
FROM (SELECT x.*
            FROM (SELECT *
                    FROM DWA.DWA_V_M_CUS_MB_USER_DERIVE T
                   WHERE T.MONTH_ID = '201801'
                     AND T.IS_INNET = '1') x) A
LEFT OUTER JOIN (select A.USER_ID,A.act_mkt_type_01,B.PURCH_MODE_TYPE_DESC 
   from (select user_id,max(act_mkt_type) as act_mkt_type_01
    from DWD.DWD_D_PRD_MB_ACT_INFO 
    WHERE MONTH_ID = '201801' 
    AND DAY_ID = '22' 
    and act_mkt_type is not null group by user_id) A
    LEFT OUTER JOIN DIM.DIM_PUB_PURCH_MODE_TYPE B 
    ON A.act_mkt_type_01=B.PURCH_MODE_TYPE) T1 --6237508
   ON A.USER_ID=T1.USER_ID
LEFT OUTER JOIN (select A.user_id,B.CODE_MKT, B.MKT_SUB_TITLE FROM
 (select USER_ID,CODE_MKT, MKT_SUB_TITLE from dwd.DWD_D_EVT_MB_MKT_ACTIVITY WHERE MONTH_ID='201801' AND DAY_ID = '22') A
   LEFT OUTER JOIN DIXIAO_MB_MKT B on A.CODE_MKT=B.CODE_MKT) T2 
   ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN (SELECT USER_ID,MAX(MKT_SUB_TITLE) AS MKT_SUB_TITLE_01 FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY WHERE MONTH_ID='201801' AND DAY_ID = '22' GROUP BY USER_ID) T3 --41363008(搞定)
   ON A.USER_ID = T3.USER_ID
LEFT OUTER JOIN (select user_id, max(is_service_mult) as is_service_mult_01 from DWA.DWA_V_M_USE_MB_PKG_USED_INFO WHERE MONTH_ID='201801' group by user_id) T4 --45003526(搞定)
   ON A.USER_ID=T4.USER_ID
LEFT JOIN (SELECT A.USER_ID,B.CODE_MKT, B.LOG_USER_MKT_SN,A.OPER_DATE,A.purch_mode_code FROM 
            (SELECT T1.USER_ID,T1.HY_TYPE,T1.purch_mode_code,T1.accept_channel_no,T1.oper_no,T1.oper_date,T1.end_date,T1.res_imei,T1.product_id,T1.service_sn FROM 
            (select a.user_id,
         CASE
                 WHEN B.PURCH_MODE_TYPE IN ('A03', 'A03', 'A15') THEN
                  '存宽带送手机'
                 WHEN B.PURCH_MODE_TYPE IN ('') THEN
                  '存费送业务'
                 WHEN B.PURCH_MODE_TYPE IN ('A17') THEN
                  'X99'
                 WHEN B.PURCH_MODE_TYPE IN ('A01', 'A16') THEN
                  '存费送机'
                 WHEN B.PURCH_MODE_TYPE IN ('A13', 'A18') THEN
                  '沃易得'
                 WHEN B.PURCH_MODE_TYPE IN ('A06') THEN
                  '存费送费'
                 WHEN B.PURCH_MODE_TYPE IN ('A14') THEN
                  '集团担保'
                 WHEN B.PURCH_MODE_TYPE IN ('') THEN
                  '其他合约'
                 WHEN B.PURCH_MODE_TYPE IN ('') THEN
                  '合约惠机'
                 WHEN B.PURCH_MODE_TYPE IN ('A02', 'A09') THEN
                  '购机送费'
                 ELSE
                  '其他合约'
               END  HY_TYPE,
         a.purch_mode_code,
         a.accept_channel_no,
         a.accept_man oper_no,
         date_tostring(a.oper_date, 'YYYYMMDD') oper_date,
         substr(a.end_date, 1, 8) end_date,
         a.res_imei,
         a.product_order product_id,
         a.service_sn,
   a.ACCEPT_AREA_NO
    from (select aa.*,row_number() over(partition by aa.user_id order by aa.oper_date desc) rn
            from dwd.dwd_d_prd_mb_act_info aa
           where aa.month_id = '201801'
             and aa.day_id = '31'
             and aa.act_mkt_type in ('A01','A02', 'A03','A09','A13','A14','A15','A16','A17') -- 终端合约
             and aa.oper_flag <> '40'
             and aa.accept_man not like 'WO00__') a
    left join dim.dim_pub_act_mkt_type b
      on a.act_mkt_type = b.purch_mode_type    
  where a.rn = 1
  union all
select a.user_id,
         case   
           when a.code_mkt in ('MKT000990', 'MKT000991', 'MKT000992') then '存费送机'  
           else
                 '购机送费'
         end HY_TYPE,
       a.code_mkt purch_mode_code,
       a.oper_dept_no accept_channel_no,
       a.oper_no,
       date_tostring(a.oper_date, 'YYYYMMDD') oper_date,
       substr(a.end_date, 1, 8) end_date,
       '' res_imei,
       '' product_id,
       a.log_user_mkt_sn service_sn,
    a.OPER_LOCAL_NET ACCEPT_AREA_NO
    from DWD.DWD_D_EVT_MB_MKT_ACTIVITY a
    left join dm.MID_D_WX_CL_ZDHY_XY_LIST_X b
    on a.user_id=b.user_id
    where code_mkt in ('MKT010071', 'MKT010072', 'MKT010073', 'MKT010074',
                    'MKT010075', 'MKT010076', 'MKT010077', 'MKT010078',
                    'MKT01007A', 'MKT01007B', 'MKT01007C', 'MKT01007D',
                    'MKT000990', 'MKT000991', 'MKT000992')
    and a.MKT_LOG_TYPE in ('10', '20')
    and a.month_id = '201801'
    and a.day_id   = '22'
    union all
 select a.user_id,
       '存费送机' HY_TYPE,
       null purch_mode_code,
       a.oper_dept_no accept_channel_no,
       a.oper_no,
       date_tostring(a.oper_date, 'YYYYMMDD') oper_date,
       substr(a.end_month, 1, 8) end_date,
       '' res_imei,
       '' product_id,
       a.oper_sn service_sn,
    a.REGION_ID ACCEPT_AREA_NO
   from (select a.*,b.REGION_ID  from dwd.dwd_d_evt_mb_mkt_back a LEFT OUTER JOIN DIM.DIM_PUB_OPER_NO b on a.OPER_DEPT_NO = b.DEPT_NO) a
   left join dm.MID_D_WX_CL_ZDHY_XY_LIST_X b
   on a.user_id=b.user_id
   where a.month_id = '201801'
   and a.day_id   = '22' 
   and a.code_mkt in ('HDTK0680', 'HDTK0681', 'HDTK0682', 'HDTK0683',
                      'HDTK0684', 'HDTK0685', 'HDTK0686'))T1) A
             LEFT OUTER JOIN (SELECT DISTINCT A.LOG_USER_MKT_SN,A.CODE_MKT
                FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY A
                WHERE A.CODE_MKT IN ('MKT032001',
                                   'MKT032002',
                                   'MKT032003',
                                   'MKT032004',
                                   'MKT032005',
                                   'MKT032006')
                  AND A.MKT_LOG_TYPE IN ('10', '20')
                  AND A.MONTH_ID = '201801'
                  AND A.DAY_ID = '22') B ON A.SERVICE_SN = B.LOG_USER_MKT_SN) T5
      ON A.USER_ID=T5.USER_ID
      
---月合并      
inser into mid_m_al_dixiao

select * from mid_m_mb_dixiao where month_id='201801'
union all
select * from mid_m_cb_dixiao where month_id='201801'



