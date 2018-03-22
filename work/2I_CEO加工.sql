--优先级:1.现场受理2.闪电购3.上门激活4.攻坚5.上门配送6.营业厅自提7.码上购8.物流

CREATE TABLE 2I_ORDER_TAGGED(
product_type string comment '2i卡种类',
order_id string comment '订单号',
order_date string comment '订单产生时间',
sign_date string comment '签收时间',
activate_date string comment '激活时间',
order_oper_date string comment '配送订单产生时间',
prov_id string comment '省份',
area_id string comment '地市',
order_status string comment '订单状态',
device_number string comment '预约号码/入网号码',
activate_status string comment '激活状态',
is_activate int comment '是否激活',
is_back_order string comment '是否退单',
activate_openid string comment '激活openid',
activate_channel_no string comment '激活渠道',
dev_no string comment '发展人编码',
activate_type int comment '激活方式/配送方式',
is_sdg int comment '是否闪电购',
is_xianchang int comment '是否现场受理',
is_shangmen int comment '是否上门配送',
is_ziti int comment '是否营业厅自提',
IS_MSG INT comment '是否码上购',
IS_XSG int comment '是否线上购',
is_jizhong int comment '是否集中配送',
is_zhuanwuliu int comment '是否转物流',
IS_TUIDAN int comment '是否退单',
IS_NOT_OPEN int comment '是否未开户退单',
month_id string,
day_id string
);



INSERT INTO TABLE 2I_ORDER_TAGGED 

SELECT 
CASE 
WHEN AO.PRODUCT_NAME LIKE '%回归%' THEN 'HUIGUI'
ELSE AO.product_type END AS product_type,
AO.order_id,
AO.order_date,
NVL(AO.sign_date, ZITI.file_dur) AS sign_date,
CASE
WHEN AO.activate_date IS NOT NULL THEN AO.activate_date
WHEN XIANCHANG.activate_TIME IS NOT NULL THEN XIANCHANG.activate_TIME
WHEN MSG.activate_TIME IS NOT NULL THEN MSG.activate_TIME
WHEN SHANGMEN.activate_date IS NOT NULL THEN SHANGMEN.activate_date
WHEN ZITI.activate_date IS NOT NULL	THEN ZITI.activate_date
WHEN SDG.activate_TIME IS NOT NULL THEN SDG.activate_TIME
WHEN XSG.activate_TIME IS NOT NULL THEN XSG.activate_TIME
ELSE NULL END AS activate_date,
CASE
WHEN XIANCHANG.order_date IS NOT NULL THEN XIANCHANG.order_date
WHEN SDG.order_date IS NOT NULL THEN SDG.order_date
WHEN SHANGMEN.order_oper_date IS NOT NULL THEN SHANGMEN.order_oper_date
WHEN ZITI.order_oper_date IS NOT NULL THEN ZITI.order_oper_date
WHEN MSG.order_oper_date IS NOT NULL THEN MSG.order_oper_date
ELSE NULL END AS order_oper_date,
AO.prov_id,
AO.area_id,
CASE
WHEN XIANCHANG.ORDER_STATUS IS NOT NULL THEN XIANCHANG.ORDER_STATUS
WHEN SDG.ORDER_STATUS IS NOT NULL THEN SDG.ORDER_STATUS
WHEN SHANGMEN.ORDER_STATUS IS NOT NULL THEN SHANGMEN.ORDER_STATUS
WHEN ZITI.ORDER_STATUS IS NOT NULL THEN ZITI.ORDER_STATUS
WHEN XSG.ORDER_STATUS IS NOT NULL THEN XSG.ORDER_STATUS
WHEN MSG.ORDER_STATUS IS NOT NULL THEN MSG.ORDER_STATUS
WHEN AO.ORDER_STATUS IS NOT NULL THEN AO.ORDER_STATUS
ELSE NULL END AS ORDER_STATUS,
AO.device_number,
CASE
WHEN XIANCHANG.activate IS NOT NULL THEN XIANCHANG.activate
WHEN SHANGMEN.activate_status IS NOT NULL THEN SHANGMEN.activate_status
WHEN ZITI.activate_status IS NOT NULL THEN ZITI.activate_status
WHEN XSG.activate IS NOT NULL THEN XSG.activate
WHEN MSG.activate_status IS NOT NULL THEN MSG.activate_status
WHEN AO.activate_status IS NOT NULL THEN AO.activate_status
ELSE NULL END AS activate_status,
CASE
WHEN XIANCHANG.activate = '已激活' THEN 1
WHEN SHANGMEN.activate_status = '已激活' THEN 1
WHEN ZITI.activate_status = '已激活' THEN 1
WHEN XSG.activate = '已激活' THEN 1
WHEN MSG.activate_status = '已激活' THEN 1
WHEN AO.activate_status = '已激活' THEN 1
ELSE 0 END AS is_activate,
AO.is_back_order,
AO.activate_openid,
AO.activate_channel_no,
CASE
WHEN XSG.dev_no IS NOT NULL THEN XSG.dev_no
WHEN XIANCHANG.dev_no IS NOT NULL THEN XIANCHANG.dev_no
WHEN SDG.dev_no IS NOT NULL THEN SDG.dev_no
WHEN SHANGMEN.dev_no IS NOT NULL THEN SHANGMEN.dev_no	--激活发展人
WHEN ZITI.oper_commend_no IS NOT NULL THEN ZITI.oper_commend_no	--开户发展人
WHEN MSG.activate_commend_no IS NOT NULL THEN MSG.activate_commend_no	--激活发展人
ELSE NULL END AS dev_no,
CASE 
WHEN XIANCHANG.order_id IS NOT NULL THEN 1
WHEN SDG.order_id IS NOT NULL THEN 2
WHEN SHANGMEN.order_id IS NOT NULL THEN 3
WHEN ZITI.order_id IS NOT NULL THEN 6
WHEN MSG.order_id IS NOT NULL THEN 7
ELSE 8 END AS activate_type,
CASE 
WHEN SDG.order_id IS NOT NULL THEN 1
ELSE 0 END AS is_sdg,
CASE 
WHEN XIANCHANG.order_id IS NOT NULL THEN 1
ELSE 0 END AS is_xianchang,
CASE 
WHEN SHANGMEN.order_id IS NOT NULL THEN 1
ELSE 0 END AS is_shangmen,
CASE 
WHEN ZITI.order_id IS NOT NULL THEN 1
ELSE 0 END AS is_ziti,
CASE 
WHEN MSG.order_id IS NOT NULL THEN 1
ELSE 0 END AS IS_MSG,
CASE
WHEN XSG.order_id IS NOT NULL THEN 1
ELSE 0 END AS IS_XSG,
CASE 
WHEN XIANCHANG.order_id IS NULL AND ZITI.order_id IS NULL AND SHANGMEN.order_id IS NULL
AND SDG.order_id IS NULL AND MSG.order_id IS NULL THEN 1
ELSE 0 END AS is_jizhong,
case 
WHEN ZITI.deliver_type ='转物流' then  1 
when SHANGMEN.DELIVER_TYPE  ='转物流' then  1 
else 0 end as is_zhuanwuliu,
CASE 
when AO.order_status like '%退单%' then 1
WHEN XIANCHANG.ORDER_STATUS like '%退单%' THEN 1
WHEN SDG.ORDER_STATUS like '%退单%' THEN 1
WHEN MSG.ORDER_STATUS like '%退单%' THEN 1
when ZITI.order_status like '%退单%' then 1 
when XSG.order_status like '%退单%' then 1 
WHEN SHANGMEN.order_status like '%退单%' then 1
else 0 end as IS_TUIDAN,  --退单标识
CASE 
WHEN AO.ORDER_STATUS IN ('审单退单','订单处理退单') THEN 1
WHEN ZITI.ORDER_STATUS IN ('审单退单','订单处理退单') THEN 1
WHEN SHANGMEN.ORDER_STATUS IN ('审单退单','订单处理退单') THEN 1
WHEN XSG.ORDER_STATUS IN ('审单退单','订单处理退单') THEN 1
ELSE 0 END AS IS_NOT_OPEN,
'201802' AS month_id,
'01' AS day_id
FROM 
lt_huangjunxin.t_2i_order_all AO	--全量订单
LEFT OUTER JOIN NEW_STG.TO_INTER_XCSL XIANCHANG ON AO.order_id = XIANCHANG.order_id	--现场受理
LEFT OUTER JOIN NEW_STG.TO_INTER_YYTZT_NEW ZITI ON AO.order_id = ZITI.order_id	--营业厅自提
LEFT OUTER JOIN NEW_STG.TO_INTER_SMJH_NEW SHANGMEN ON AO.order_id = SHANGMEN.order_id	--上门激活
LEFT OUTER JOIN NEW_STG.TO_INTER_SDG SDG ON AO.order_id = SDG.order_id	--闪电购
LEFT OUTER JOIN NEW_STG.TO_INTER_MSG_NEW MSG ON AO.order_id = MSG.order_id	--码上购
LEFT OUTER JOIN NEW_STG.TO_INTER_XSG XSG ON AO.order_id = XSG.order_id --线上购
;
