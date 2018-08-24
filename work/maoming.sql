create table roam_user_list_maoming
(device_number string,
product_name string,
province_name string,
start_date string,
end_date string,
stay_days string,
city_name string,
area_name string,
prof_name string,
is_jk string,
realname_flag string,
user_status_prov_desc string,
pay_mode_desc string,
cust_name string,
pspt_id string,
id_address string,
cert_type_prov_desc string,
oper_dept_no string,
channel_name string,
innet_date string,
channel_type string,
back_dept_desc string,
is_pay string,
imei string,
imei_if_hekui string,
imei_if_zb string,
if_roam string,
if_local string,
if_focus string,
if_normal string,
user_id string,
flag_type string,
zj_times bigint,
zj_dura decimal(20,0),
dire_sms bigint,
is_transfer_call string,
other_number string,
total_flux decimal(22,2),
out_jf_times bigint,
out_cnt bigint,
in_jf_times bigint,
in_cnt bigint,
out_sms_num bigint,
ws_sms string,
qq_cnt double,
by_lac_num bigint,
not_by_lac_num bigint,
qn_cdr_nums bigint,
qw_cdr_nums bigint,
qn_lac_nums bigint,
qw_lac_nums bigint,
fee1 double,
fee2 double,
fee3 double,
fee4 double,
fee5 double,
fee6 double)
partitioned by (month_id string, day_id string);