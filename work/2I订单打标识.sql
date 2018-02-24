truncate table dm_2I_order_detail;
insert into dm_2I_order_detail 
( 
   ORDER_CODE, 
   order_month, 
   ORDER_DATE, 
   ORDER_TIME, 
   province,                -------------------- 
   CITY, 
   area,                  -------------------- 
   cust_name,             -------------------- 
   sex, 
   age, 
   ORDER_STATUS, 
   book_num, 
   ADDR, 
   package_name,         -------------------- 
   PRODUCT_NAME, 
   SVC_NUMBER, 
   send_time,           -------------------- 
   is_audit,            -------------------- 
   PROJECT_TYPE, 
   ACTIVATE_STATUS, 
   ACTIVATE_date, 
   ACTIVATE_TIME, 
   SIGN_TIME, 
   open_type, 
   send_type, 
   sign_type, 
   ziti_flag, 
   smjh_flag, 
   develop_code, 
   jizhong_flag, 
   kds_flag, 
   td_flag, 
   zytd_open_flag, 
   yytzt_open_flag, 
   smjh_open_flag, 
   zwl_flag, 
   ziti_zwl_flag, 
   smjh_zwl_flag, 
   xcsh_flag, 
   order_develop, 
   spread_dev,
   xcsl_develop, 
   dispatch_time， 
   open_time, 
   channel_code, 
   user_id,
   book_id,
   book_order_code,
   book_card_id 
) 

(
SELECT T.ORDER_CODE, 
        decode(t.order_time, null, SUBSTR(oo.BOOK_TIME, 1, 6), SUBSTR(T.ORDER_TIME, 1, 6) ) AS order_month, 
        decode(t.order_time, null, SUBSTR(oo.BOOK_TIME, 1, 8), SUBSTR(T.ORDER_TIME, 1, 8) ) AS ORDER_DATE, 
        SUBSTR(T.ORDER_TIME, 1, 8) || SUBSTR(T.ORDER_TIME, 10, 2) || 
SUBSTR(T.ORDER_TIME, 13, 2) || SUBSTR(T.ORDER_TIME, 16, 2) as ORDER_TIME  , 
        t.province,              -------------------- 
        decode(T.CITY, null, oo.city, t.city) as city, 
        t.area,                  -------------------- 
        t.cust_name,             -------------------- 
        t.sex, 
        t.age, 
        T.ORDER_STATUS, 
        t.book_num, 
        t.shipping_addr, 
        t.package_name,          -------------------- 
        T.PRODUCT_NAME, 
        T.SVC_NUMBER, 
        SUBSTR(T.send_time, 1, 8) as send_time, -------------------- 
        t.is_audit,         -------------------- 
        T.PROJECT_TYPE, 
        case 
           when T.ACTIVATE_STATUS = '已激活' then  1 
           else 0 
        end as ACTIVATE_STATUS , --是否状态 
        substr(T.ACTIVATE_TIME,1,8) as ACTIVATE_date,     --激活日期 
        substr(T.ACTIVATE_TIME,1,8) || substr(T.ACTIVATE_TIME,10,2) || SUBSTR(T.ACTIVATE_TIME, 13, 2) || SUBSTR(T.ACTIVATE_TIME, 16, 2) as ACTIVATE_TIME,     --激活时间 
        case 
           when IS_DATE_FULK(T.SIGN_TIME) = 1 then 
substr(T.SIGN_TIME,1,8) || substr(T.SIGN_TIME,10,2) || 
SUBSTR(T.SIGN_TIME, 13, 2) || SUBSTR(T.SIGN_TIME, 16, 2) 
           when IS_DATE_FULK1(N.SIGN_TIME) = 1 then 
substr(N.SIGN_TIME,1,4) || substr(N.SIGN_TIME,6,2) || 
substr(N.SIGN_TIME,9,2) || substr(N.SIGN_TIME,12,2) || 
substr(N.SIGN_TIME,15,2)|| substr(N.SIGN_TIME,15,2) 
           else null 
        end as SIGN_TIME ,  --签收时间 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN S.IS_TIMEOUT IN ('他人签收','本人签收') then  1 
           when O.DELIVER_TYPE IN ('他人签收','本人签收')then 1 
           when T.ORDER_STATUS IN ('成功关闭', '发货退单', '发货中', 
'客户拒收退单', '未签收', '物流退单', '物流在途', '待发货') then 1 
           when T.ORDER_STATUS ='系统退单' AND (T.PROJECT_TYPE != 'M' OR 
(T.PROJECT_TYPE = 'M' AND  T.PAY_STATUS ='已付款') ) then 1 
           when IS_DATE_FULK(T.SEND_TIME) = 1 then 1 
           when IS_DATE_FULK(T.SIGN_TIME) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_TIME) = 1 then 1 
           when s.open_time like '20%' then 1 
           when o.open_time like '20%' then 1 
           when m.open_time like '20%' then 1  
           else 0 
        end as open_type,  --开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN  S.IS_TIMEOUT IN ('他人签收','本人签收') then  1 
           when O.DELIVER_TYPE IN ('他人签收','本人签收')then 1 
           when IS_DATE_FULK(T.SEND_TIME) = 1 then 1 
           when IS_DATE_FULK(T.SIGN_TIME) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_TIME) = 1 then 1 
           else 0 
        end as send_type,  --发货标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN  S.IS_TIMEOUT IN ('他人签收','本人签收') then  1 
           when O.DELIVER_TYPE IN ('他人签收','本人签收')then 1 
           when IS_DATE_FULK(T.SIGN_TIME) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_TIME) = 1 then 1 
           else 0 
        end as sign_type,  --签收标识 
        case 
           when s.order_code is not null  and s.dept_name not like '%华盛仓%' then 1 
           else 0 
        end as ziti_flag,  --自提标识 
        case 
           when o.order_code is not null  then 1 
           else 0 
        end as smjh_flag,  --上门标识 
        o.develop_code,    --上门发展人编码 

         case 
           when o.order_code is  null and s.order_code is  null and m.order_code is null then 1 
           WHEN S.IS_TIMEOUT ='转物流' then  1 
           WHEN S.Deliver_Type ='转物流' then  1 
           when O.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as jizhong_flag,  --集中标识 
        case 
            when  m.ORDER_CODE is not null then 0 
            else 
                  decode((select t.city from dm_2i_city_area m where m.city = t.city  and m.area = t.area ), null , 1, 0) 
        end as kds_flag,  --跨地市标识 
        case 
           when s.order_status like '%退单%' then 1 
           WHEN o.order_status like '%退单%' then 1
           when t.order_status like '%退单%' then 1
           else 0 
        end as td_flag,  --退单标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when s.open_time is not null and s.open_time like '20%' then 1 
           WHEN o.open_time is not null and o.open_time like '20%' then 1 
           else 0 
        end as zytd_open_flag,  --自有团队开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when s.open_time is not null and s.open_time like '20%' then 1 
           else 0 
        end as yytzt_open_flag,  --营业厅自提开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when o.open_time is not null and o.open_time like '20%' then 1 
           else 0 
        end as smjh_open_flag,  --上门激活开户标识 
        case 
           WHEN S.IS_TIMEOUT ='转物流' then  1 
           WHEN S.Deliver_Type ='转物流' then  1 
           when O.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as zwl_flag,  --转物流标识 
        case 
           WHEN S.IS_TIMEOUT ='转物流' then  1 
           WHEN S.Deliver_Type ='转物流' then  1 
           else 0 
        end as ziti_zwl_flag,  --自提转物流标识 
        case 
           when O.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as smjh_zwl_flag,  --上门转物流标识 
        case 
           when  m.ORDER_CODE is not null  then 1 
           else 0 
        end as xcsh_flag,  --现场受理标识 
        case 
          when m.develope_code is not null then m.develope_code 
          when o.develop_code is not null then o.develop_code 
          when j.developer_code is not null then j.developer_code
          else null   
        end as order_develop,  --订单发展人编码，优先取现场受理的发展人编码 
       case 
           when k.prom_dev_code is not null then k.prom_dev_code
          when m.develope_code is not null then m.develope_code 
          when o.develop_code is not null then o.develop_code 
          when j.developer_code is not null then j.developer_code
          else null   
        end as spread_dev,  --推广发展人编码
        m.develope_code,  --现场受理发展人编码 
        o.dispatch_time,   --派单时间 
        case 
              when m.open_time like '201%' then substr(m.open_time, 1, 8) 
              when o.open_time like '201%' then substr(o.open_time, 1, 8)
              when s.open_time like '201%' then substr(s.open_time, 1, 8)
              else null 
         end  as open_time , ---------开户时间 
         s.channel_code,  ---自提营业厅编码 
         t.user_id,  -------用户编码
         oo.book_id,        ---预约ID
         oo.order_code as book_order_code, --预约订单编号
         oo.card_no as book_card_id
         

   FROM DM_2I_ORDER T   ---全量订单
   FULL JOIN DM_2I_ORD_ZITI S ON  S.ORDER_CODE = T.ORDER_CODE ---营业厅自提
   FULL JOIN DM_2I_ORD_VISIT_ACTIVE O ON O.ORDER_CODE = T.ORDER_CODE  --上门激活
/*   FULL JOIN T_2I_DELIVER_DETAIL N ON N.ORDER_CODE = T.ORDER_CODE    ---交付明细
*/   full join dm_2i_accept_detail m ON m.ORDER_CODE = T.ORDER_CODE   ---现场受理
   full join dm_2i_ord_sdg j ON j.ORDER_CODE = T.ORDER_CODE   --闪电购
   full join dm_2i_ord_xsg k ON k.ORDER_CODE = T.ORDER_CODE   --线上购
   full join dm_2i_order_book oo on oo.ORDER_CODE = t.ORDER_CODE   --意向单
 );
 
 
commit;