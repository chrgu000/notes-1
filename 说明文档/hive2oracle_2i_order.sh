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
select * from lt_mengguanzhou.unbind_user;
" > 2i_order_data.txt

echo "
load data
characterset utf8
infile '2i_order_data.txt'
append
into table unbind_user
fields terminated by X'09'
TRAILING NULLCOLS
(
 day_id,
 user_id,
 device_number,
 in_date,
 prov_code,
 prov_name,
 city_code,
 city_name,
 contact_phone,
 unbind_type,
 unbind_state,
 chnl_type
)
" > 2i_order_data.ctl

sqlldr userid=$v_user/$v_passwd@newjfdb control=2i_order_data.ctl

rm 2i_order_data.txt
rm 2i_order_data.ctl