CREATE TABLE `open_076_ao.ods_d_zb_cus_ds_label_all_user`(
CYCLE_ID_D	VARCHAR2(40)  comment '日账期',
KD_BANDWIDTH	NUMBER  comment '宽带速率',
USER_ID	VARCHAR2(40)  comment '用户编码',
DEVICE_NUMBER	varchar2(20) comment  '用户号码',
KD_AGE	NUMBER  comment '宽带用户网龄',
USER_STATUS  NUMBER comment '用户状态',
CHINL_DESC  VARCHAR2(40) comment '入网渠道',
KD_COMB_SUB_TYPE NUMBER comment '宽带融合类型',
KD_SERVICE_TYPE NUMBER comment '当前使用套餐产品',
IS_COMB_SERVICE NUMBER comment '是否融合业务捆绑',
IS_THIS_DEV	NUMBER comment '是否当月新发展',
IS_THIS_MONTH_DEV NUMBER '是否当日新发展',
KD_CHINL_ID VARCHAR2(40) comment '宽带渠道类型编码',
KD_BALANCE NUMBER comment '宽带账户余额',
KD_MOBILE_NUMBER1 VARCHAR2(30) '宽带捆绑移网用户1',
KD_LV6_ADDR VARCHAR2(40) '宽带六级地址名称',
KD_CUS_MNGER_NAME VARCHAR2(10) '宽带客户经理姓名',
KD_COMB_START_TIME VARCHAR2(10) '宽带融合开始时间',
KD_END_MONTH VARCHAR2(10) '宽带到期月'
)  
  
COMMENT '[]'
PARTITIONED BY(MONTH_ID STRING COMMENT '??ݷ??',DAY_ID STRING COMMENT '????')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
NULL DEFINED AS ''
STORED AS RCFILE;