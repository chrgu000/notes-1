-----张日鑫，黎振霖

truncate table dm_d_al_waihu_user1;
insert into table dm_d_al_waihu_user1

  select a.user_no,
  nvl(b.is_guangxi,0) is_guangxi,
  c.USE_ID_TYPE,
  nvl(d.is_cybjl,0) is_cybjl,
  e.std_charge,
  f.P2P_FEE,
  g.MMS_FEE,
  h.pay_charge,
  i.device_number as virtual_number,
  j.ACCESS_SPEED,
  k.is_lost,
  nvl(l.is_kd_mb,0) is_kd_mb,
  m1.pay_charge_day,
  m.OPER_NO as wx_OPER_NO,
  n.manager_desc as wx_manager_desc,
  nvl(o.is_yuyue,0) is_yuyue,
  p.CREDIT_NUMBER,
  q.six_m_arpu,
  r.thr_m_max,
  nvl(s.is_black,0) is_black,
  nvl(t.is_red,0) is_red,
  u.imei,
  v.34_msisdn,
  nvl(w.is_tuoshou,0) is_tuoshou
  from (select * from (select user_no,cust_id,device_number from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801' and flag='0'
         union all
       select user_id as user_no,cust_id,device_number from dwa.DWA_V_M_CUS_CB_USER_INFO where month_id='201801' and is_innet='1')c) a
  left join (select b.user_id,
    case when c.user_id is not null then '1' else '0' end is_guangxi 
    from dwa.DWA_V_M_CUS_MB_USER_DERIVE b 
    left outer join (select user_id from dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST a where a.month_id='201801' and a.day_id='22' 
      and  a.USE_ID_TYPE='ID001' and a.use_id_no like '45%') c 
            on b.user_id=c.user_id where b.month_id='201801' and b.is_innet='1') b ----------是否广西用户
  on a.user_no=b.user_id
  left join (select o.USER_ID,o.USE_ID_TYPE 
    from (select a.USER_ID as USER_ID,a.MONTH_ID as MONTH_ID,b.USE_ID_TYPE as USE_ID_TYPE from dwa.DWA_V_M_CUS_CB_USER_INFO  a 
      left outer join dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST b 
      on a.USER_ID = b.USER_ID
      where a.month_id='201801' and b.month_id='201801' and b.day_id='22'
      union all 
      select DISTINCT n.USER_ID as USER_ID,
      n.MONTH_ID as MONTH_ID,m.USE_ID_TYPE as USE_ID_TYPE from dwa.DWA_V_M_CUS_MB_USER_DERIVE  n 
      left outer join dwa.DWA_V_D_CUS_AL_SMZ_USER_LIST m on n.USER_ID = m.USER_ID where n.month_id='201801' 
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
  left join (select a.USER_NO,b.std_charge from dwd.DWD_d_PRD_AL_USER_INFO a 
    left outer join (select std_charge,product_id from dim.DIM_REF_PRODUCT_TYPE) b
    on a.PRODUCT=b.product_id 
                    where a.month_id='201801' and a.day_id='22') e ----------主套餐固定费用
  on a.user_no=e.user_no 
  left join (select f.USER_NO,t.P2P_FEE 
    from (select user_no from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801') f 
    left outer join (select USER_ID as USER_ID,P2P_FEE as P2P_FEE from DWA.DWA_V_M_CUS_CB_CHARGE where month_id='201801'
     union all select USER_ID as USER_ID,P2P_FEE as P2P_FEE from DWA.DWA_V_M_CUS_MB_CHARGE where month_id='201801') t
                    on f.USER_NO=t.user_id) f ----------发送短信费用
  on a.user_no=f.user_no
  left join (select f.USER_NO,t.MMS_FEE from 
    (select user_no from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801') f 
    left outer join (select USER_ID as USER_ID,MMS_FEE as MMS_FEE from DWA.DWA_V_M_CUS_CB_CHARGE where month_id='201801'
     union all select USER_ID as USER_ID,MMS_FEE as MMS_FEE from DWA.DWA_V_M_CUS_MB_CHARGE where month_id='201801' ) t
                    on f.USER_NO=t.USER_ID) g ---------发送彩信费用
  on a.user_no=g.user_no
         left join (select user_id,sum(pay_charge) as pay_charge from dwd.DWD_D_ACC_AL_PAY where month_id='201801' and day_id='22' group by user_id) h ------缴费金额-----重复(已修改)
         on a.user_no=h.user_id
         left join (
          select aa.comp_id,bb.device_number from (select comp_id,usenum from (select tem.comp_id,count(tem.comp_id) as usenum from (select comp_id,user_id,service_type,sub_svc_type,is_innet,is_valid from dwa.DWA_V_D_CUS_AL_COMB_MEMBERS where month_id='201801' and day_id='22' and is_valid=1 and COMP_TYPE in ('50','61','62','63','64','64','66','67','70','71','81','82','83'))tem group by tem.comp_id) tt where usenum>1) aa left outer join (select user_no,device_number from dwd.DWD_d_PRD_AL_USER_INFO where month_id='201801' and day_id='22') bb on aa.comp_id=bb.user_no
         ) i  -----------虚拟号码----重复(已修改)
         on a.user_no=i.comp_id
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
         left join (select USER_ID,case when kd_num=0 then 0 else 1 end as is_kd_mb 
           from (select n.USER_ID,sum(case when n.MKT_SUB_TITLE like '%宽带送手机%' then 1 else 0 end) as kd_num 
             from dwd.DWD_D_PRD_MB_MKT_INFO n 
             where n.month_id='201801' and n.day_id='22' and n.is_valid=1 group by n.USER_ID) tem ) l   ------------------存宽带送手机用户----重复 1是，0否(已修改）
         on a.user_no=l.user_id
         left join (select user_id,count(distinct wrtoff_date) as pay_charge_day from dwd.DWD_D_ACC_AL_PAY_EXT where month_id='201801'
          and day_id='22' group by user_id) m1 ---------缴费天数（当月有缴费的天数）
         on a.user_no=m1.user_id
         left join (select b.user_id,b.OPER_NO from dwd.DWD_D_CUS_AL_AGENT_USER_REL b where month_id='201801' and day_id='22') m --------维系工号
         on a.user_no=m.user_id
         left join (select distinct user_id,manager_desc from dwd.DWD_D_CUS_AL_AGENT_USER_REL a
          left outer join 
                    dim.DIM_PUB_JZYX_MANAGER_NO b on a.OPER_NO=b.manager_no where a.MONTH_ID='201801' and a.day_id='22') n ---------维系经理名字----重复(已修改)
         on a.user_no=n.user_id
         left join (select USER_ID,case when 
          NEXT_DEAL_FLAG not in ('9','E','F') and cancel_flag='0' then '1'
          else '0' end is_yuyue from dwd.DWD_D_EVT_CB_TRADE_HIS where month_id='201801' and day_id='22'
          union all
                    select tem.user_id,case when tem.yuyue=0 then 1 else 0 end as is_yuyue from (select user_id,min(start_flag) as yuyue from dwd.DWD_D_EVT_AL_USER_SER_DINNER where month_id='201801' and day_id='22' group by user_id) tem) o --------------是否存在预约工单-----重复(已修改)
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
                     from (select customer_no,max(create_date) as num from dwd.DWD_D_CUS_AL_BLACK_LIST where month_id='201801' and customer_no is not null and day_id='22' group by customer_no) ww) s --------是否黑名单---重复(已修改) 1是；0否
         on a.cust_id=s.customer_no
         left join  ( select user_id,case when user_id is not null then '1'
                      else '0' end is_red from DWD.DWD_D_CUS_AL_VIP where month_id='201801' and user_id is not null and day_id='22') t --------是否红名单 1是；0否
         on a.user_no=t.user_id
         left join (select b.imei,a.user_id from dwa.DWA_V_M_CUS_MB_USER_DERIVE a left outer join 
           (select imei,msisdn from dwd.DWD_M_RES_MB_TUPLE_FIVE where month_id='201801') b 
           on a.DEVICE_NUMBER=b.msisdn
                            where a.month_id='201801' and a.IS_INNET='1') u -----------终端串号
         on a.user_no=u.user_id
         left join (select s.msisdn as 34_msisdn from 
          (select substr(a.imei, 1, 8) as imei_pre,msisdn from dwd.DWD_M_RES_MB_TUPLE_FIVE a where month_id='201801') s
          left semi join 
          (select substr(b.imei, 1, 8) as imei_pre,unicom_4g,unicom_3g,unicom_2g from dim.DIM_PUB_FT_IMEI b where unicom_4g='1' or unicom_3g='1' ) t 
                    on s.imei_pre=t.imei_pre) v                          ------3,4G终端用户的设备号码---查一下主表的用户号码有没有重复
         on a.DEVICE_NUMBER=v.34_msisdn
         left join (select a.USER_NO,case when b.tuoshou_num=0
          then '0' else '1' end is_tuoshou 
          from dwd.DWD_M_PRD_AL_USER_INFO a 
          left outer join (select tem.user_id,sum(case when tem.pay_type in ('003','004','013','020','054','055','068','069','C01','B03','C07','C31','G02','G03','WZF') then 1 else 0 end) as tuoshou_num from (select user_id,pay_type from dwd.DWD_D_ACC_AL_ACCT where month_id='201801' and day_id='22') tem group by tem.user_id) b
                    on a.USER_NO=b.user_id  where a.month_id='201801') w  ------托收用户标识，1为托收,0为现金和其他cb没有托收-------重复(已修改)
         on a.user_no=w.user_no
         where a.month_id='201801';
         
truncate table dm_d_al_waihu_user3;       
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
from (select * from (select user_no,cust_id,device_number from dwd.DWD_M_PRD_AL_USER_INFO where month_id='201801' and flag='0'
         union all
       select user_id as user_no,cust_id,device_number from dwa.DWA_V_M_CUS_CB_USER_INFO where month_id='201801' and is_innet='1')c) a
         left join (select A.user_id,case when C.product_id in ('G6080','G6093','90064126','90259327','OS050')
                    or C.product_name like 'M2M%' or B.industry_flag=1 then '1' else '0' end special_flag 
                    from dwa.DWA_V_M_CUS_MB_USER_DERIVE A 
                    left outer join dim.DIM_PUB_PRODUCT_ID B
                    on A.product_id=B.product
                    left outer join dim.DIM_REF_PRODUCT_TYPE C on A.product_id=C.product_id where A.month_id='201801' and is_innet='1') x --------特殊用户标识（ESIM、物联网、隐私号、行业应用）
        on a.user_no=x.user_id
        left join (select USER_NO,a.channel_no,case when b.area_kind in ('02','03') then '1' else '0' end is_town
                   from dwd.DWD_M_PRD_AL_USER_INFO a 
                   left outer join (select channel_no,area_kind from dwd.DWD_M_PUB_AL_CHANNEL where month_id='201801' and is_valid='1') b
                   on a.channel_no= b.channel_no where a.month_id='201801' and a.flag='0') y -------是否乡镇属性
        on a.user_no=y.user_no
        left join (select a.user_id,a.tel,b.registertime from
                  (select user_id,DEVICE_NUMBER as tel from dwa.DWA_V_M_CUS_MB_USER_DERIVE where IS_INNET='1' and MONTH_ID='201801') a 
                   left outer join (select msisdn as tel,registertime from dwd.DWD_M_RES_MB_TUPLE_FIVE where month_id='201801') b
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
                   select distinct a.user_no,a.month_id from dwd.DWD_M_PRD_AL_USER_INFO a left join ods.log_info_lease b on a.user_no=b.user_no
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
       where a.month_id='201801';
       
       
       
---蒙冠州话务表

--目标表：DWA_A_D/M_PRO_MB_USER、DWA_A_D/M_PRO_CB_USER

--中间表 语音MID_M_MB_VOICE_S
INSERT INTO table MID_M_MB_VOICE_S

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


INSERT INTO table MID_M_CB_VOICE_S

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
INSERT INTO table MID_M_MB_NO_VOICE

select user_id,month_id,
case when substr('201802',5,2) in('01','03','05','07','08','10','12') then 31-NO_VOICE_DAYS
when substr('201802',1,4)%4='0' and substr('201802',5,2)='02' then 29-NO_VOICE_DAYS
when substr('201802',1,4)%4<>'0' and substr('201802',5,2)='02' then 28-NO_VOICE_DAYS
else 30-NO_VOICE_DAYS end NO_VOICE_DAYS from(
select MONTH_ID,USER_ID,count(DISTINCT DAY_ID) AS NO_VOICE_DAYS from 
dwd.DWD_D_USE_MB_VOICE
where month_id = '201802'
GROUP BY MONTH_ID,USER_ID) c  

INSERT INTO table MID_M_CB_NO_VOICE
select user_id,month_id,
case when substr('201802',5,2) in('01','03','05','07','08','10','12') then 31-NO_VOICE_DAYS
when substr('201802',1,4)%4='0' and substr('201802',5,2)='02' then 29-NO_VOICE_DAYS
when substr('201802',1,4)%4<>'0' and substr('201802',5,2)='02' then 28-NO_VOICE_DAYS
else 30-NO_VOICE_DAYS end NO_VOICE_DAYS from(
select MONTH_ID,USER_ID,count(DISTINCT DAY_ID) AS NO_VOICE_DAYS from
dwd.DWD_D_USE_CB_VOICE
where month_id = '201802'
GROUP BY MONTH_ID,USER_ID) c



--中间表 机场基站
insert into table mid_m_airport_bs

select * from 
(select * from DWD.DWD_D_RES_AL_BASE_STATION WHERE month_id = '201802' and day_id = '06'
and CAB_TYPE <= 120 and LONGITUDE <= 26) t
WHERE GetDistance(CAB_TYPE,LONGITUDE,108.18539,22.6044136) <= 8000
or GetDistance(CAB_TYPE,LONGITUDE,110.0428157,25.2210564) <= 8000
or GetDistance(CAB_TYPE,LONGITUDE,109.2921887,21.5392272) <= 8000
or GetDistance(CAB_TYPE,LONGITUDE,109.3933546,24.2070736) <= 8000
or GetDistance(CAB_TYPE,LONGITUDE,111.2480996,23.4562667) <= 8000
or GetDistance(CAB_TYPE,LONGITUDE,106.9609931,23.7202848) <= 8000;


--中间表 每日是否出现在机场
INSERT INTO table MID_D_MB_AIRPORT_DAYS

select USER_ID,count(*) as is_airport from 
(select month_id,user_id,day_id,cell_id,lac_id 
from dwa.DWA_S_D_USE_MB_BS where month_id ='201801' AND DAY_ID = '22') a 
right outer join mid_m_airport_bs b 
on a.cell_id = b.lac_id and a.lac_id = b.BASE_MARK 
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
NVL(F.ROAM_BS_NUM, 0) AS ROAM_BS_NUM_D,	--漫游基站数
NVL(I.ACTIVE_BS_NUM, 0) AS ACTIVE_BS_NUM_D,--使用活跃基站数
NVL(K.xianshi_flux, 0) AS xianshi_flux_D,	--闲时流量使用量
NVL(L.FREE_LIMIT_REMAIN, 0) AS FREE_LIMIT_REMAIN_D,	--可用流量
NVL(M.CALL_10010, 0) AS CALL_10010_D, --拨打客服次数
NVL(Q.free_limit_used,0) AS DX_FLUX_USED_D,--定向流量使用量
CASE 
WHEN NVL(O.roam_bs_num,0) > 0 THEN 1
ELSE 0 END AS IS_ROAM_TODAY,--是否漫游
CASE 
WHEN NVL(P.is_airport, 0) > 0 THEN 1
ELSE 0 END AS is_airport --是否出现在机场
FROM
(SELECT USER_ID,MONTH_ID,DAY_ID,DEVICE_NUMBER FROM DWD.DWD_D_PRD_MB_NET_USER_INFO
WHERE MONTH_ID = '201801' AND DAY_ID = '22' AND STATUS NOT IN ('41','42') and is_innet='1') A
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
NVL(D.VOICE_OUT,0) AS VOICE_OUT_D,	--使用套外语音分钟数
'0' SEND_MMS_NUM_D,	--发送彩信数
NVL(E.roam_bs_num,0) AS roam_bs_num_D,	--漫游基站数
NVL(F.ACTIVE_BS_NUM,0) AS ACTIVE_BS_NUM_D,	--活跃基站数
'0' DX_FLUX_USED_D,	--定向流量使用量
NVL(G.xianshi_flux,0) AS xianshi_flux_D,	--闲时流量使用量
'0' FREE_LIMIT_REMAIN_D,	--可用流量
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
MID_D_MB_AIRPORT_DAYS K
ON
A.USER_ID = K.USER_ID;



--BSS_M
INSERT INTO
DWA_A_M_PRO_MB_USER 

select
A.USER_ID,
A.MONTH_ID,	--月份
A.DEVICE_NUMBER,	--电话号码
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX, 0) AS FLUX_PRO_USED, --套餐内使用流量
NVL(B.FLUX_4G, 0) AS FLUX_4G, --4g基站流量
NVL(B.TOTAL_FEE, 0) AS TOTAL_FEE, --超出套餐流量费用
NVL(B.BILL_FLUX, 0) AS BILL_FLUX,--超出主套餐的流量
NVL(A1.SMS_OUT, 0) AS SMS_OUT, --套餐外短信数量
NVL(C.BZ_FEE_FLUX, 0) AS BZ_FEE_FLUX, --标准资费流量
NVL(B.DOWN_ROAM_CONT_FLUX + B.UP_ROAM_CONT_FLUX, 0) AS ROAM_COUNT_FLUX, --国内漫游使用流量
NVL(A1.SEND_SMS_NUM, 0) AS SEND_SMS_NUM,	--发送短信数
NVL(D.VOICE_OUT, 0) AS VOICE_OUT,	--使用套外语音分钟数
NVL(D.CALL_TIMES, 0) AS CALL_TIMES,	--通话次数
NVL(D.CALLING_TIMES, 0) AS CALLING_TIMES,	--主叫通话时长
--NVL(G.lac_id, '--') AS lac_id,	--常住基站lac
--NVL(G.cell_id, '--') AS cell_id,	--常住基站cell_id
NVL(J.jiaowang_nums, 0) AS jiaowang_nums,	--交往圈数
DAY(date_add('2018-02-01',-1)) - NVL(K.NO_VOICE_DAYS, 0) AS NO_VOICE_DAYS, --未产生话单天数
NVL(N.xianshi_flux, 0) AS xianshi_flux,--闲时流量使用量
NVL(PRO_D.DX_FLUX_USED, 0) AS DX_FLUX_USED,--定向流量使用量
NVL(PRO_D.CALL_10010,0) AS CALL_10010,--拨打客服次数
NVL(PRO_D.SEND_MMS_NUM,0) AS SEND_MMS_NUM,--发送彩信数
NVL(PRO_D.roam_day,0) AS roam_day,--漫游天数
NVL(PRO_D.AIRPORT_DAYS,0) AS AIRPORT_DAYS--出现在机场天数
FROM 
(SELECT USER_ID,DEVICE_NUMBER,MONTH_ID FROM DWD.DWD_M_PRD_MB_NET_USER_INFO
WHERE MONTH_ID = '201801' AND STATUS NOT IN ('41','42') and is_innet='1' and is_innet='1') A
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
MAX(DX_FLUX_USED_D) AS DX_FLUX_USED,
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
A.MONTH_ID,	--月份
NVL(A1.DEVICE_NUMBER, '--') AS DEVICE_NUMBER,
NVL(B.PROD_IN_LOCAL_FLUX + B.PROD_IN_ROAM_CONT_FLUX + B.PROD_IN_ROAM_GAT_FLUX + B.PROD_IN_ROAM_INT_FLUX,0) AS FLUX_PRO_USED, --套餐内使用流量
NVL(B.FLUX_4G,0),  --4g基站流量
NVL(B.DOWN_ROAM_CONT_FLUX + B.UP_ROAM_CONT_FLUX,0) AS ROAM_COUNT_FLUX, --国内漫游使用流量
NVL(B.TOTAL_FEE,0) AS TOTAL_FEE, --超出套餐流量费用
NVL(B.BILL_FLUX,0) AS BILL_FLUX, --超出主套餐的流量
NVL(A1.SMS_OUT,0) AS SMS_OUT,  --套餐外短信数量
NVL(A1.SEND_SMS_NUM,0) AS SEND_SMS_NUM, --发送短信数
NVL(D.VOICE_OUT,0) AS VOICE_OUT,	--使用套外语音分钟数
NVL(D.CALL_TIMES,0) AS CALL_TIMES,	--通话次数
NVL(D.CALLING_TIMES,0) AS CALLING_TIMES,	--主叫通话时长
'0' SEND_MMS_NUM,	--发送彩信数
-----F.lac_id,	--常驻基站lac
------F.cell_id,	--常住基站cell_id
NVL(I.jiaowang_nums,0) AS jiaowang_nums,	--交往圈数
DAY(date_add('2018-02-01',-1)) - NVL(J.NO_VOICE_DAYS, 0) AS NO_VOICE_DAYS,	--未产生话单天数
NVL(H.ACTIVE_BS_NUM,0) AS ACTIVE_BS_NUM,  --使用活跃基站数
'0' DX_FLUX_USED,	--定向流量使用量
NVL(K.xianshi_flux,0) AS xianshi_flux,	--闲时流量使用量
NVL(PRO_D.BZ_FEE_FLUX,0) AS BZ_FEE_FLUX,--标准资费流量
NVL(PRO_D.CALL_10010,0) AS CALL_10010,--拨打客服次数
NVL(PRO_D.roam_day,0) AS roam_day,--漫游天数
NVL(PRO_D.AIRPORT_DAYS,0) AS AIRPORT_DAYS--出现在机场天数
FROM
(SELECT USER_ID,MONTH_ID FROM DWA.DWA_V_M_CUS_CB_USER_INFO
WHERE MONTH_ID = '201801' and is_innet='1') A
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

--truncate table channel_list_gx 
insert into table channel_list_gx 
select d.CHANNEL_NO,d.CHANNEL_NAME from 
(select t.*,row_number()over(partition by t.channel_no order by t.is_only_valid) rn
 from DWD.DWD_D_PUB_AL_CHANNEL t
 where month_id='201803' and day_id='25') d where d.rn<=1;
 
--truncate table mid_d_mb_dixiao
insert into table mid_d_mb_dixiao
SELECT
    A.USER_ID AS USER_ID,
    '0' AS is_ice,--是否办理畅越冰激凌,
    (CASE WHEN T1.SERVICE_NAME is not null THEN '1' ELSE '0' END) AS is_flux_pkg,--是否订购流量包,
    T1.CHANNEL_NAME  AS FLUX_CHANNEL_TYPE,  --流量包受理渠道,
    T1.CHANNEL_NO AS FLUX_CHANNEL_ID,--流量包受理渠道编码,
    NULL AS ICE_PRODUCT_ID,--畅越冰激凌产品代码,
    '' AS ICE_ACCEPT_DATE,--畅越冰激凌受理时间,
    '' AS ICE_TRADE_STAFF_ID,--越冰激凌受理工号,
    '' AS ICE_TRADE_DEPART_ID,--畅越冰激凌受理渠道编码,
    '0' AS is_cybjl,--是否办理畅越冰激凌,
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
 WHERE T.MONTH_ID = '201803' 
 AND T.DAY_ID = '25'
 AND T.IS_INNET = '1') A
 
    LEFT OUTER JOIN 
(select a.user_id,a.SERVICE_CODE,a.SERVICE_NAME, b.CHANNEL_NO, b.CHANNEL_NAME 
from (SELECT e.*,row_number() over(partition by user_id order by OPER_DATE desc)as rn FROM DWA.DWA_S_D_USE_MB_PKG_GPRS_USED e WHERE MONTH_ID='201802' AND DAY_ID='28' AND SERVICE_NAME like '%流量包%')a 
left join channel_list_gx b on a.OPER_DEPT_NO=b.CHANNEL_NO where A.rn<=1) T1 
ON A.USER_ID=T1.USER_ID

 
    LEFT OUTER JOIN 
(select A.user_id,A.CODE_MKT,A.MKT_SUB_TITLE,A.CHANNEL_ID,A.OPER_NO,B.CHANNEL_NAME from
(select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_PRD_MB_MKT_INFO b where code_mkt IN ('MKT108701','MKT108702','MKT108703','MKT108704') and month_id='201803' and day_id='25' and START_DATE<='20180122' and END_DATE>='20180122') A  
left outer join channel_list_gx B ON A.CHANNEL_ID=B.CHANNEL_NO  where A.rn <=1) T3
ON A.USER_ID = T3.USER_ID
  
    LEFT OUTER JOIN 
(SELECT A.USER_ID,A.OPER_DATE,A.OPER_NO,B.CODE_MKT,B.MKT_SUB_TITLE 
  FROM 
    (select * from(
select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_EVT_MB_MKT_ACTIVITY b where month_id='201803' and day_id='25' and BEGIN_DATE <='20180122' and END_DATE>='20180122') a  
where rn <=1) A
 INNER JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT) T5 
ON A.USER_ID=T5.USER_ID
    
    LEFT OUTER JOIN 
 (SELECT E.*, C.CHANNEL_NO,C.CHANNEL_NAME FROM 
  (SELECT A.USER_ID,A.CHANNEL_ID,B.CODE_MKT,B.MKT_SUB_TITLE
  FROM 
  (select b.*, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY OPER_DATE desc) AS rn from DWD.DWD_D_PRD_MB_MKT_INFO b where month_id='201803' and day_id='25' and START_DATE<='20180122' and END_DATE >='20180122') A
  RIGHT JOIN DIXIAO_MB_MKT B 
  ON A.CODE_MKT=B.CODE_MKT WHERE A.rn <=1) E 
  left outer join channel_list_gx C ON E.CHANNEL_ID=C.CHANNEL_NO) T6 
ON A.USER_ID=T6.USER_ID
LEFT OUTER JOIN (SELECT * FROM dm.DM_D_WX_CL_ZDHY_XY_LIST WHERE MONTH_ID='201803' AND DAY_ID='25') T7 ON A.USER_ID=T7.USER_ID; 



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
WHERE T.MONTH_ID = '201803' 
AND T.DAY_ID = '25'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN (SELECT D.USER_ID,D.CHANNEL_ID,D.FLUX_PKG,E.CHNL_NAME
FROM 
(SELECT T.USER_ID,C.CHANNEL_ID,C.FLUX_PKG
FROM (select b.*,row_number() over(partition by user_id order by ACCEPT_TIME desc) as rn from DWA.DWA_V_D_CUS_CB_DISCNT_INFO b where month_id='201803' and day_id='25' and START_DATE <='20180122' and END_DATE >='20180122') T
inner join (select USER_ID,PRODUCT_ID,PACKAGE_ID,FLUX_PKG,CHANNEL_ID from DWA.DWA_V_D_CUS_CB_DIY_PACKAGE 
WHERE MONTH_ID = '201803' 
AND DAY_ID = '25' 
AND FLUX_PKG IS NOT NULL) C ON T.USER_ID=C.USER_ID WHERE T.rn <=1) D
left outer join dim.DIM_PUB_AGENT_ZB_REL E ON D.CHANNEL_ID=E.CHNL_CODE WHERE E.MAIN_FLAG='1')T3 
ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN
  (select * from (select b.*,row_number() over(partition by user_id order by ACCEPT_DATE desc) as rn from DWD.DWD_D_EVT_CB_TRADE_PRODUCT b where month_id='201803' and day_id='25' AND START_DATE <='20180122' AND END_DATE>='20180122' AND PRODUCT_ID IN('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) c where c.rn<=1) T4
ON A.USER_ID=T4.USER_ID;


--truncate table mid_d_cb_dixiao2
insert into table mid_d_cb_dixiao2

select
a.user_id,
T5.PRODUCT_NAME AS ICE_PRODUCT_NAME,--畅越冰激凌名称,
(substr(T5.PRODUCT_NAME, instr(T5.PRODUCT_NAME, '-') + 1, instr(T5.PRODUCT_NAME, '元') - instr(T5.PRODUCT_NAME, '-') - 1)) AS ice_dangwei,--畅越冰激凌档位,
'0' AS IS_LIUSHI,--是否受理流失预警,
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
WHERE T.MONTH_ID = '201803' 
AND T.DAY_ID = '25'
AND T.IS_INNET = '1'
AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN 
(SELECT A.USER_ID,A.PRODUCT_ID,B.PRODUCT_NAME FROM
   (select b.*,row_number() over(partition by user_id order by ACCEPT_DATE desc) as rn from DWD.DWD_D_EVT_CB_TRADE_PRODUCT b where month_id='201803' and day_id='25' AND START_DATE <='20180122' AND END_DATE>='20180122' AND PRODUCT_ID IN ('90311370', '90311373', '90343397', '90343398', '90343399', '90343400', '90343401', '90343402', '90343403', '90343404', '90343405', '90343406', '90343407', '90343408', '90343409', '90343410', '90343411', '90343412', '90343420')) A
        LEFT OUTER JOIN
        (SELECT PRODUCT_ID,PRODUCT_NAME FROM DWD.DWD_D_PRD_CB_PRODUCT WHERE MONTH_ID = '201803' AND DAY_ID = '25' AND START_DATE<='20180122' AND END_DATE >='20180122') B 
         ON A.PRODUCT_ID=B.PRODUCT_ID where A.rn<=1) T5
ON A.USER_ID=T5.USER_ID
LEFT OUTER JOIN
(SELECT D.*, C.CHNL_NAME
FROM (SELECT A.ID AS user_id,A.ACCEPT_DATE,A.TRADE_DEPART_ID,A.TRADE_STAFF_ID,B.PRODUCT_ID,B.PACKAGE_ID,B.DISCNT_CODE,B.MKT_SUB_TITLE,B.DISCNT_NAME 
   FROM 
   (SELECT b.*, row_number() over(partition by id ORDER BY accept_date desc ) as rn  FROM DWD.DWD_D_EVT_CB_TRADE_DISCNT b WHERE MONTH_ID='201803' AND DAY_ID='25') A 
     INNER JOIN DIXIAO_CB_MKT B 
     ON A.PRODUCT_ID=B.PRODUCT_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.DISCNT_CODE=B.DISCNT_CODE WHERE A.rn<=1) D
     LEFT OUTER JOIN dim.DIM_PUB_AGENT_ZB_REL C ON D.TRADE_DEPART_ID=C.CHNL_CODE WHERE C.MAIN_FLAG='1') T7 
ON A.USER_ID=T7.user_id
LEFT OUTER JOIN (SELECT * FROM dm.DM_D_WX_CL_ZDHY_XY_LIST WHERE MONTH_ID='201803' AND DAY_ID='25') T8 ON A.USER_ID=T8.USER_ID;



     
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

--truncate table mid_d_al_dixiao      
insert into table mid_d_al_dixiao
select user_id,
is_ice,
is_flux_pkg,
flux_channel_type,
flux_channel_id,
ice_product_id,
ice_accept_date,
ice_trade_staff_id,
ice_trade_depart_id,
nvl(is_cybjl,'0') as is_cybjl,
ice_product_name,
ice_dangwei,
nvl(is_liushi,'0') as is_liushi,
liushi_oper_no,
liushi_mkt_sub_title,
liushi_code_mkt,
liushi_channel_id,
liushi_channel_type,
hy_type,
hy_accept_area_no,
hy_oper_no,
hy_chnl_name,
hy_chnl_code,
is_auto_renew,
dixiao_accept_date,
dixiao_trade_staff_id,
dixiao_discnt_code,
dixiao_product_name,
is_accept_dixiao,
dixiao_trade_depart_id,
dixiao_channel_type
 from (
select * from mid_d_mb_dixiao
union all
select * from mid_d_cb_dixiao)c



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
   WHERE T.MONTH_ID = '201802'
             AND T.IS_INNET = '1'
             AND T.SERVICE_TYPE IN ('04AAAAAA', '40AAAAAA')) A
LEFT OUTER JOIN (SELECT A.ID AS user_id,A.ACCEPT_DATE,B.PRODUCT_ID,B.PACKAGE_ID,B.DISCNT_CODE,B.MKT_SUB_TITLE,B.DISCNT_NAME 
   FROM 
   (SELECT b.*, row_number() over(partition by id ORDER BY accept_date desc ) as rn  FROM DWD.DWD_D_EVT_CB_TRADE_DISCNT b WHERE MONTH_ID='201802' AND DAY_ID='28' AND START_DATE<='20180228' AND END_DATE>='20180228') A 
     LEFT OUTER JOIN DIXIAO_CB_MKT B 
     ON A.PRODUCT_ID=B.PRODUCT_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.DISCNT_CODE=B.DISCNT_CODE  WHERE A.rn<=1) T2
ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN  (SELECT A.USER_ID,A.PRODUCT_ID, A.PACKAGE_ID,A.DISCNT_CODE,B.DISCNT_NAME FROM
 (select b.*,row_number() over(partition by user_id order by start_date desc) as rn from dwd.DWD_D_PRD_CB_USER_DISCNT b WHERE MONTH_ID='201802' AND DAY_ID = '28' and end_date>='20180228' and
  start_date<='20180228') A 
 LEFT OUTER JOIN (SELECT DISCNT_CODE,DISCNT_NAME FROM zbg_src.SRC_D_BCD07003 WHERE MONTH_ID ='201803' and day_id='17' and discnt_name not 
 like '%开通4G%') B ON A.DISCNT_CODE=B.DISCNT_CODE where A.rn<=1) T3 
 ON A.USER_ID=T3.USER_ID
LEFT OUTER JOIN (SELECT T.USER_ID,C.CHANNEL_ID,C.FLUX_PKG
FROM (select b.*,row_number() over(partition by user_id order by ACCEPT_TIME desc) as rn from DWA.DWA_V_D_CUS_CB_DISCNT_INFO b where month_id='201802' and day_id='28' and START_DATE <='20180228' and END_DATE >='20180228') T
inner join (select USER_ID,PRODUCT_ID,PACKAGE_ID,FLUX_PKG,CHANNEL_ID from DWA.DWA_V_D_CUS_CB_DIY_PACKAGE 
WHERE MONTH_ID = '201802' 
AND DAY_ID = '28' 
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
  CASE  WHEN (T5.PURCH_MODE_CODE IS NOT NULL OR T5.LOG_USER_MKT_SN IS NOT NULL) THEN '1' ELSE '0' end is_auto --是否自动续约
FROM (SELECT x.*
            FROM (SELECT *
                    FROM DWA.DWA_V_M_CUS_MB_USER_DERIVE T
                   WHERE T.MONTH_ID = '201802'
                     AND T.IS_INNET = '1') x) A
LEFT OUTER JOIN (select A.USER_ID,A.act_mkt_type,B.PURCH_MODE_TYPE_DESC,A.END_DATE
   from (select d.*,row_number() over(partition by user_id order by START_DATE desc) as rn
    from DWD.DWD_D_PRD_MB_ACT_INFO d 
    WHERE MONTH_ID = '201802' 
    AND DAY_ID = '28' 
    and START_DATE <='20180228'
    and END_DATE>='20180228'
    and act_mkt_type is not null) A
    LEFT OUTER JOIN DIM.DIM_PUB_PURCH_MODE_TYPE B 
    ON A.act_mkt_type=B.PURCH_MODE_TYPE WHERE A.rn<=1) T1
   ON A.USER_ID=T1.USER_ID
LEFT OUTER JOIN 
(select A.user_id,B.CODE_MKT, B.MKT_SUB_TITLE FROM
 (select e.*,row_number() over(partition by user_id order by OPER_DATE desc) as rn 
 from dwd.DWD_D_EVT_MB_MKT_ACTIVITY e
 WHERE MONTH_ID='201802' AND DAY_ID = '28' AND BEGIN_DATE<='20180228' AND END_DATE>='20180228') A
   left outer join DIXIAO_MB_MKT B on A.CODE_MKT=B.CODE_MKT where A.rn<=1) T2 
   ON A.USER_ID=T2.USER_ID
LEFT OUTER JOIN (SELECT A.USER_ID,A.CODE_MKT,A.MKT_SUB_TITLE FROM 
     (SELECT b.*, row_number() over(partition by USER_ID ORDER BY OPER_DATE desc ) as rn FROM DWD.DWD_D_EVT_MB_MKT_ACTIVITY b WHERE MONTH_ID='201802' AND DAY_ID = '28' AND BEGIN_DATE<='20180228' AND END_DATE>='20180228') A WHERE A.rn<=1) T3 --41363008(搞定)
   ON A.USER_ID = T3.USER_ID
LEFT OUTER JOIN (select user_id, max(is_service_mult) as is_service_mult_01 from DWA.DWA_V_M_USE_MB_PKG_USED_INFO WHERE MONTH_ID='201802' group by user_id) T4
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
  AND A.MONTH_ID = '201802'
  AND A.DAY_ID = '28') D
   ON B.SERVICE_SN = D.LOG_USER_MKT_SN) T5
ON A.USER_ID=T5.USER_ID;

---合并
--truncate table mid_m_al_dixiao      
insert into table mid_m_al_dixiao
select * from (
select * from mid_m_mb_dixiao
union all
select * from mid_m_cb_dixiao)c
      



