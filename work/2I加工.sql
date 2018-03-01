
UPDATE dwd.DWD_D_EVT_MB_TO_INTER_ORDER SET EXISTS = 1 WHERE ORDER_ID IN SELECT ORDER_ID FROM TABLE_2I_TAGGED;

insert into TABLE_2I_TAGGED 
( 
   ORDER_ID, 
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
   ACTIVATE_DATE, 
   SIGN_DATE, 
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
   dispatch_time, 
   open_time, 
   channel_code, 
   user_id,
   book_id,
   book_order_code,
   book_card_id 
) 

(
SELECT T.ORDER_ID, 
		SUBSTR(NVL(regexp_replace(T.order_time, '-', ''), BOOXSG.BOOK_TIME), 1, 6) AS ORDER_MONTH,
		NVL(T.ORDER_TIME, SUBSTR(BOOXSG.BOOK_TIME, 1, 4) + '-' + SUBSTR(BOOXSG.BOOK_TIME, 5, 2) + '-' 
			+ SUBSTR(BOOXSG.BOOK_TIME, 7)) AS ORDER_DATE,
        T.ORDER_TIME,
        t.province,              -------------------- 
        NVL(T.CITY, BOOXSG.CITY) as city, 
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
        substr(T.ACTIVATE_DATE,1,8) as ACTIVATE_DATE,     --激活日期 
        T.ACTIVATE_DATE as ACTIVATE_TIME,     --激活时间 
        case 
           when T.SIGN_DATE IS NOT NULL then 
			substr(T.SIGN_DATE,1,8) || substr(T.SIGN_DATE,10,2) || 
			SUBSTR(T.SIGN_DATE, 13, 2) || SUBSTR(T.SIGN_DATE, 16, 2) 
           when N.SIGN_DATE IS NOT NULL then 
			substr(N.SIGN_DATE,1,4) || substr(N.SIGN_DATE,6,2) || 
			substr(N.SIGN_DATE,9,2) || substr(N.SIGN_DATE,12,2) || 
			substr(N.SIGN_DATE,15,2)|| substr(N.SIGN_DATE,15,2) 
           else null 
        end as SIGN_TIME,  --签收时间 
        case
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN ZITI.deliver_type IN ('他人签收','本人签收') then  1 
           when SMJH.DELIVER_TYPE IN ('他人签收','本人签收') then 1 
           when T.ORDER_STATUS IN ('成功关闭', '发货退单', '发货中', 
			'客户拒收退单', '未签收', '物流退单', '物流在途', '待发货') then 1,
           when T.ORDER_STATUS ='系统退单' AND (T.product_type != 'M' OR 
				(T.product_type = 'M' AND  T.is_valid_order ='是') ) then 1 
           when IS_DATE_FULK(T.SEND_TIME) = 1 then 1 
           when IS_DATE_FULK(T.SIGN_DATE) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_DATE) = 1 then 1 
           when ZITI.open_time like '20%' then 1 
           when SMJH.open_time like '20%' then 1 
           when XCSL.open_time like '20%' then 1 
           else 0 
        end as open_type,  --开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN  ZITI.IS_TIMEOUT IN ('他人签收','本人签收') then  1 
           when SMJH.DELIVER_TYPE IN ('他人签收','本人签收')then 1 
           when IS_DATE_FULK(T.SEND_TIME) = 1 then 1 
           when IS_DATE_FULK(T.SIGN_DATE) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_DATE) = 1 then 1 
           else 0 
        end as send_type,  --发货标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           WHEN  ZITI.IS_TIMEOUT IN ('他人签收','本人签收') then  1 
           when SMJH.DELIVER_TYPE IN ('他人签收','本人签收')then 1 
           when IS_DATE_FULK(T.SIGN_DATE) = 1 then 1 
           when IS_DATE_FULK1(N.SIGN_DATE) = 1 then 1 
           else 0 
        end as sign_type,  --签收标识 
        case 
           when ZITI.order_code is not null  and ZITI.dept_name not like '%华盛仓%' then 1 
           else 0 
        end as ziti_flag,  --自提标识 
        case 
           when SMJH.order_code is not null  then 1 
           else 0 
        end as smjh_flag,  --上门标识 
        SMJH.develop_code,    --上门发展人编码 

         case 
           when SMJH.order_code is  null and ZITI.order_code is  null and XCSL.order_code is null then 1 
           WHEN ZITI.IS_TIMEOUT ='转物流' then  1 
           WHEN ZITI.Deliver_Type ='转物流' then  1 
           when SMJH.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as jizhong_flag,  --集中标识 
        case 
            when  XCSL.ORDER_CODE is not null then 0 
            else 
                  decode((select t.city from dm_2i_city_area m where XCSL.city = t.city  and XCSL.area = t.area ), null , 1, 0) 
        end as kds_flag,  --跨地市标识 
        case 
           when ZITI.order_status like '%退单%' then 1 
           WHEN SMJH.order_status like '%退单%' then 1
           when t.order_status like '%退单%' then 1
           else 0 
        end as td_flag,  --退单标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when ZITI.open_time is not null and ZITI.open_time like '20%' then 1 
           WHEN SMJH.open_time is not null and SMJH.open_time like '20%' then 1 
           else 0 
        end as zytd_open_flag,  --自有团队开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when ZITI.open_time is not null and ZITI.open_time like '20%' then 1 
           else 0 
        end as yytzt_open_flag,  --营业厅自提开户标识 
        case 
           when T.ACTIVATE_STATUS = '已激活'then 1 
           when SMJH.open_time is not null and SMJH.open_time like '20%' then 1 
           else 0 
        end as smjh_open_flag,  --上门激活开户标识 
        case 
           WHEN ZITI.IS_TIMEOUT ='转物流' then  1 
           WHEN ZITI.Deliver_Type ='转物流' then  1 
           when SMJH.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as zwl_flag,  --转物流标识 
        case 
           WHEN ZITI.IS_TIMEOUT ='转物流' then  1 
           WHEN ZITI.Deliver_Type ='转物流' then  1 
           else 0 
        end as ziti_zwl_flag,  --自提转物流标识 
        case 
           when SMJH.DELIVER_TYPE  ='转物流' then  1 
           else 0 
        end as smjh_zwl_flag,  --上门转物流标识 
        case 
           when  XCSL.ORDER_CODE is not null  then 1 
           else 0 
        end as xcsh_flag,  --现场受理标识 
        case 
          when XCSL.develope_code is not null then XCSL.develope_code 
          when SMJH.develop_code is not null then SMJH.develop_code 
          when SDG.developer_code is not null then SDG.developer_code
          else null   
        end as order_develop,  --订单发展人编码，优先取现场受理的发展人编码 
       case 
           when XSG.prom_dev_code is not null then XSG.prom_dev_code
          when XCSL.develope_code is not null then XCSL.develope_code 
          when SMJH.develop_code is not null then SMJH.develop_code 
          when SDG.developer_code is not null then SDG.developer_code
          else null   
        end as spread_dev,  --推广发展人编码
        XCSL.develope_code,  --现场受理发展人编码 
        SMJH.dispatch_time,   --派单时间 
        case 
              when XCSL.open_time like '201%' then substr(XCSL.open_time, 1, 8) 
              when SMJH.open_time like '201%' then substr(SMJH.open_time, 1, 8)
              when ZITI.open_time like '201%' then substr(ZITI.open_time, 1, 8)
              else null 
         end  as open_time , ---------开户时间 
         ZITI.channel_code,  ---自提营业厅编码 
         t.user_id,  -------用户编码
         BOOK.book_id,        ---预约ID
         BOOK.order_code as book_order_code, --预约订单编号
         BOOK.card_no as book_card_id
         

   FROM (SELECT * FROM dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE EXISTS = 0) T   ---全量订单
   FULL JOIN DM_2I_ORD_ZITI ZITI ON  ZITI.ORDER_ID = T.ORDER_ID ---营业厅自提
   FULL JOIN DM_2I_ORD_VISIT_ACTIVE SMJH ON SMJH.ORDER_ID = T.ORDER_ID  --上门激活
/*   FULL JOIN T_2I_DELIVER_DETAIL N ON N.ORDER_CODE = T.ORDER_ID    ---交付明细
*/   full join dm_2i_accept_detail XCSL ON XCSL.ORDER_CODE = T.ORDER_ID   ---现场受理
   full join dm_2i_ord_sdg SDG ON SDG.ORDER_CODE = T.ORDER_ID   --闪电购
   full join dm_2i_ord_xsg XSG ON XSG.ORDER_CODE = T.ORDER_ID   --线上购
   full join dm_2i_order_book BOOK on BOOK.ORDER_CODE = t.ORDER_ID   --意向单
 );