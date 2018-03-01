SELECT T4.ORG_MANAGER_TYPE_DESC , t1.*
     FROM bcsecnew.BCC_EC_PREPARE_CALC_OBJ T1, 
          bcsecnew.BCC_EC_CODE_OBJ_TYPE T4 
    WHERE T1.OBJ_TYPE = T4.ORG_MANAGER_TYPE
      and t1.obj_state=5;

select area_id,count(distinct order_id) from dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE (month_id = '201802'
or month_id = '201801') and (order_date like '2018-01%' or order_date like '2018-02') group by area_id;

select area_id,count(distinct order_id) from dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE month_id = '201802' 
and day_id = '01' and order_date like '2018-01%' group by area_id;

select area_id,count(distinct order_id) from dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE month_id = '201802' 
and day_id = '28' and order_date like '2018-01%' group by area_id;

select area_id,count(distinct order_id) from dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE (month_id = '201802' 
and day_id = '01') or (month_id = '201802' and day_id = '28') and (order_date like '2018-01%' 
or order_date like '2018-02%') group by area_id;

select * from dwd.DWD_D_EVT_MB_TO_INTER_ORDER WHERE (month_id = '201802'
or month_id = '201801') and (order_date like '2018-02%' or order_date like '2018-02') limit 30;