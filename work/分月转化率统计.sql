SELECT      
'地市',            
'订单量',               
'开户量',               
'开户率',               
'发货量',               
'发货率',               
'签收量',               
'签收率',               
'激活量',               
'激活率',               
'充值量',               
'转化率'      
FROM DUAL

UNION ALL

select 
SS.地市,     
TO_CHAR( SS.订单量 ) as 订单量,     
TO_CHAR( SS.开户量 ) as 开户量,     
TO_CHAR( DECODE(SS.订单量, 0, 0, TRUNC(SS.开户量 * 100 / SS.订单量, 2) ) ) || '%'  AS 开户率,     
TO_CHAR( SS.发货量 ) as 发货量,     
TO_CHAR( DECODE(SS.开户量, 0, 0, TRUNC(SS.发货量 * 100 / SS.开户量, 2) ) ) || '%'  AS 发货率,     
TO_CHAR( SS.签收量 ) as 签收量,     
TO_CHAR( DECODE(SS.发货量, 0, 0, TRUNC(SS.签收量 * 100 / SS.发货量, 2) ) ) || '%'  AS 签收率,     
TO_CHAR( SS.激活量 ) as 激活量,     
TO_CHAR( DECODE(SS.签收量, 0, 0, TRUNC(SS.激活量 * 100 / SS.签收量, 2) ) ) || '%'  AS 激活率,     
TO_CHAR( SS.充值量 ) as 充值量,     
TO_CHAR( DECODE(SS.订单量, 0, 0, TRUNC(SS.充值量 * 100 / SS.订单量, 2) ) ) || '%' AS 转化率  
from
(
SELECT
T.ORDER_MONTH                                       AS 月份,     
T.CITY                                              AS 地市,     
count(1)                                           AS 订单量,     
SUM(
  case 
    when t.order_code is not null  and T.OPEN_TYPE = 1 then 1
    else 0
  end
)                                                  AS 开户量,     
SUM(
  case 
    when t.order_code is not null  and T.SEND_TYPE = 1 then 1
    else 0
  end
)                                                  AS 发货量, 

SUM(
  case 
    when t.order_code is not null  and T.SIGN_TYPE = 1 then 1
    else 0
  end
)                                                  AS 签收量, 

SUM(
  case 
    when t.order_code is not null  and T.ACTIVATE_STATUS = 1 then 1
    else 0
  end
)                                                  AS 激活量,       
SUM(     
CASE     
       WHEN t.order_code is not null and T.PROJECT_TYPE = 'M' AND T.ACTIVATE_STATUS = 1 THEN 1 
       WHEN t.order_code is not null and T.ACTIVATE_STATUS = 1 AND T.PRODUCT_NAME IN ('大王超级会员卡', '腾讯大王卡回归') THEN 1      
       WHEN t.order_code is not null and S.TOTAL_CHARGE_NUMBER >=:TOTAL_MONNEY * 100  THEN 1      
       ELSE 0     
END     
)                                                    AS 充值量     
     
FROM DM_2I_ORDER_DETAIL T  ---订单表   
LEFT JOIN FULIANKUN_2I_NUMBER_CHARGE S ON T.USER_ID = S.USER_ID    --充值表  
WHERE 1=1     
AND T.ORDER_DATE >=:START_TIME             
AND T.ORDER_DATE <=:END_TIME 
AND:CITY_NAME  IN (SELECT T.CITY FROM DUAL  UNION SELECT  '全区' FROM DUAL)   
AND:PROJECT_TYPE  IN (SELECT T.PROJECT_TYPE FROM DUAL  UNION SELECT  'ALL' FROM DUAL)        
GROUP BY T.CITY, T.ORDER_MONTH   

union all

SELECT 
T.ORDER_MONTH                                       AS 月份,    
'全区'                                              AS 地市,     
count(1)                                           AS 订单量,     
SUM(
  case 
    when t.order_code is not  null  and T.OPEN_TYPE = 1 then 1
    else 0
  end
)                                                  AS 开户量,     
SUM(
  case 
    when t.order_code is not null  and T.SEND_TYPE = 1 then 1
    else 0
  end
)                                                  AS 发货量, 

SUM(
  case 
    when t.order_code is not null  and T.SIGN_TYPE = 1 then 1
    else 0
  end
)                                                  AS 签收量, 

SUM(
  case 
    when t.order_code is not null  and T.ACTIVATE_STATUS = 1 then 1
    else 0
  end
)                                                  AS 激活量,       
SUM(     
CASE     
       WHEN t.order_code is not null and T.PROJECT_TYPE = 'M' AND T.ACTIVATE_STATUS = 1 THEN 1 
       WHEN t.order_code is not null and T.ACTIVATE_STATUS = 1 AND T.PRODUCT_NAME IN ('大王超级会员卡', '腾讯大王卡回归') THEN 1      
       WHEN t.order_code is not null and S.TOTAL_CHARGE_NUMBER >=:TOTAL_MONNEY * 100  THEN 1      
       ELSE 0     
END     
)                                                    AS 充值量     
     
FROM DM_2I_ORDER_DETAIL T     
LEFT JOIN FULIANKUN_2I_NUMBER_CHARGE S ON T.USER_ID = S.USER_ID     
WHERE 1=1     
AND T.ORDER_DATE >=:START_TIME             
AND T.ORDER_DATE <=:END_TIME  
AND:CITY_NAME  IN (SELECT T.CITY FROM DUAL  UNION SELECT  '全区' FROM DUAL)   
AND:PROJECT_TYPE  IN (SELECT T.PROJECT_TYPE FROM DUAL  UNION SELECT  'ALL' FROM DUAL) 
GROUP BY T.ORDER_MONTH   
) ss 
