#!/bin/bash 

##********************************************
#声明变量,变量赋值
v_date=$1
if [ -n "$v_date" ]; then echo ""; else v_date=$(date -d '1 day ago' +%Y%m%d) ;fi
echo "v_date:$v_date"
v_month_id=`echo $v_date | cut -c 1-6`
echo "v_month_id:$v_month_id"
v_last_day=`echo $v_date | cut -c 7-8`
echo "v_last_day:$v_last_day"

v_user=wx
v_passwd=qzhu_wx_2017

source /usr/local/oracle/xe/oracle_env

hive -e "
select * from DWD.DWD_D_EVT_MB_TO_INTER_ORDER where month_Id = '$v_month_id' and day_id='$v_last_day';
" > 2i_order_data.txt

echo "
load data
characterset utf8
infile '2i_order_data.txt'
append
into table DWD_D_EVT_MB_TO_INTER_ORDER
fields terminated by X'09'
TRAILING NULLCOLS
(
product_type,
order_id,
order_date,
prov_id,
area_id,
pay_charge,
charge,
order_status,
cust_name,
card_id,
order_device_number,
sex,
age,
product_name,
goods_name,
device_number,
delivery_addr,
delivery_city_no,
logistics_supplier,
logistics_id,
send_date,
logistics_status,
sign_date,
activate_status,
audit_status,
activate_date,
cust_ip,
audit_remarks CHAR(4000),
order_back_reason,
charge_back_reason,
sms_detail CHAR(4000),
is_send,
is_acti_to_oper_audit,
is_acti_to_oper,
is_valid_order,
is_back_order,
call_dur,
sms_num,
tencent_score,
activate_openid,
activate_channel_no,
dev_no,
order_openid,
order_channel_no,
product_id,
month_id,
day_id
)
" > 2i_order_data.ctl

sqlldr userid=$v_user/$v_passwd@newjfdb control=2i_order_data.ctl

rm 2i_order_data.txt
rm 2i_order_data.ctl