hive -e "use lt_mengguanzhou;
create table MID_ROAM_QQ_SUM as select * from lt_huangjunxin.MID_ROAM_QQ_SUM limit 1;
truncate table MID_ROAM_QQ_SUM;
create table MID_ROAM_PAY as select * from lt_huangjunxin.MID_ROAM_PAY limit 1;
truncate table MID_ROAM_PAY;
create table mid_roam_backuser as select * from lt_huangjunxin.mid_roam_backuser limit 1;
truncate table mid_roam_backuser;
create table hy_voice_numbers_imei as select * from lt_huangjunxin.hy_voice_numbers_imei limit 1;
truncate table hy_voice_numbers_imei;
create table hy_voice_jf_times_imei as select * from lt_huangjunxin.hy_voice_jf_times_imei limit 1;
truncate table hy_voice_jf_times_imei;
create table hy_flux_imei as select * from lt_huangjunxin.hy_flux_imei limit 1;
truncate table hy_flux_imei;
create table TP_LH_0626_ROAM_LIST_2 as select * from lt_huangjunxin.TP_LH_0626_ROAM_LIST_2 limit 1;
truncate table TP_LH_0626_ROAM_LIST_2;
create table TP_LH_0626_ROAM_LIST_4 as select * from lt_huangjunxin.TP_LH_0626_ROAM_LIST_4 limit 1;
truncate table TP_LH_0626_ROAM_LIST_4;" 2>&1 |tee create_maoming.log >>/dev/null