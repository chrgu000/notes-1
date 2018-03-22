SELECT  
t.city,
count(1)                                            as 订单量,       
SUM(T.OPEN_TYPE)                                   AS 开户量,    
SUM(T.SEND_TYPE)                                   AS 发货量,    
SUM(T.SIGN_TYPE)                                   AS 签收量,    
SUM(T.ACTIVATE_STATUS)                            AS 激活量,    
SUM(    
CASE    
       WHEN T.PROJECT_TYPE = 'M' AND T.ACTIVATE_STATUS = 1 and s.total_charge_number>=19*100 THEN 1
       WHEN T.ACTIVATE_STATUS = 1 AND T.PRODUCT_NAME IN ( '腾讯大王卡回归') THEN 1      
       WHEN  T.ACTIVATE_STATUS = 1  and S.TOTAL_CHARGE_NUMBER >=20*100  THEN 1     
       ELSE 0    
END    
)
                                                    AS 充值量,
SUM( 
CASE           
        WHEN T.ACTIVATE_STATUS =1  AND d.USER_ID IS NULL THEN 1          
        ELSE 0           
END            
)                                                     AS 王卡助手绑定量
    
FROM DM_2I_ORDER_DETAIL T    
LEFT JOIN FULIANKUN_2I_NUMBER_CHARGE S ON T.USER_ID = S.USER_ID
left join  dm_order_develop_info n on t.order_develop=n.order_develop and n.state =1
left join  DM_2I_UNBIND_USER d on T.USER_ID = d.USER_ID
where 1=1
AND T.ORDER_DATE >='20180301'            
AND T.ORDER_DATE <='20180307'
and t.order_code is not null
group by t.city
order by t.city