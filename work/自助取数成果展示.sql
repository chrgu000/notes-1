SELECT DAY_ID, 
LOGIN_USER_CNT,
DL_CNT,
LOGIN_CNT,
mkfw
  FROM (SELECT COUNT(*) LOGIN_CNT,
               COUNT(DISTINCT USER_ID) LOGIN_USER_CNT,
               TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') DAY_ID
          FROM VBAP.pure_log_enter@NEWJFDB a
         GROUP BY TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD')) A,
       (SELECT COUNT(*) mkfw, to_char(a.visit_DATE, 'YYYYMMDD') visit_DATE
          FROM VBAP.pure_log_visit@NEWJFDB a, VBAP.PURE_RESOURCES@NEWJFDB b
         where a.resources_id = b.resources_id
           AND resources_name IN
               ('个性化定制', '自助报表', '即席查询', '我的应用', '应用商店')
         group by to_char(a.visit_DATE, 'YYYYMMDD')) G,
       
       (SELECT COUNT(*) DL_CNT, TO_CHAR(a.start_date, 'YYYYMMDD') START_DATE
          FROM VBAP.VBAP_TASK_INSTANCE@NEWJFDB A, VBAP.VBAP_TASK_DEFINITION@NEWJFDB B
         WHERE A.TASK_ID = B.TASK_ID
         GROUP BY TO_CHAR(a.start_date, 'YYYYMMDD')) E
 WHERE A.DAY_ID = G.visit_DATE(+)
   AND A.DAY_ID = E.START_DATE(+)

 ORDER BY DAY_ID;
 

SELECT DAY_ID, 
LOGIN_USER_CNT,---登录用户数 
DL_CNT, ---导出文件数
LOGIN_CNT, --登陆次数
mkfw ----模块访问次数
  FROM (SELECT COUNT(*) LOGIN_CNT,
               COUNT(DISTINCT USER_ID) LOGIN_USER_CNT,
               TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') DAY_ID
          FROM VBAP.pure_log_enter@NEWJFDB a ----结果表名
        ---WHERE TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') = '20160629'
         GROUP BY TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD')) A,
       (SELECT COUNT(*) mkfw, to_char(a.visit_DATE, 'YYYYMMDD') visit_DATE
          FROM VBAP.pure_log_visit@NEWJFDB a, VBAP.PURE_RESOURCES@NEWJFDB b
         where a.resources_id = b.resources_id
           AND resources_name IN
               ('个性化定制', '自助报表', '即席查询', '我的应用', '应用商店')
         group by to_char(a.visit_DATE, 'YYYYMMDD')) G,
       
       (SELECT COUNT(*) DL_CNT, TO_CHAR(a.start_date, 'YYYYMMDD') START_DATE
          FROM VBAP.VBAP_TASK_INSTANCE@NEWJFDB A, VBAP.VBAP_TASK_DEFINITION@NEWJFDB B
         WHERE A.TASK_ID = B.TASK_ID
         GROUP BY TO_CHAR(a.start_date, 'YYYYMMDD')) E
 WHERE A.DAY_ID = G.visit_DATE(+)
   AND A.DAY_ID = E.START_DATE(+)

 ORDER BY DAY_ID;


 select sum(loginNum) from 
(select 
       role_name,count(*) as loginNum
from
       (select user_id from vbap.pure_log_enter where login_date between to_timestamp('2017-10-01 00:00:0.000000000',
'yyyy-mm-dd hh24:mi:ss.ff9') and to_timestamp('2017-11-01 00:00:0.000000000','yyyy-mm-dd hh24:mi:ss.ff9')) e,
       (select * from vbap.pure_role) r,
       (select * from vbap.pure_user_role) u
where
       e.user_id = u.user_id
and
       u.role_id = r.role_id
group by
      r.role_name
order by 
      role_name)
where role_name like '北海%';


SELECT month_id, 
LOGIN_USER_CNT,
DL_CNT,
LOGIN_CNT,
mkfw
  FROM (SELECT COUNT(*) LOGIN_CNT,
               COUNT(DISTINCT USER_ID) LOGIN_USER_CNT,
               TO_CHAR(A.LOGIN_DATE, 'YYYYMM') month_id
          FROM VBAP.pure_log_enter@NEWJFDB a
         GROUP BY TO_CHAR(A.LOGIN_DATE, 'YYYYMM')) A,
       (SELECT COUNT(*) mkfw, to_char(a.visit_DATE, 'YYYYMM') visit_DATE
          FROM VBAP.pure_log_visit@NEWJFDB a, VBAP.PURE_RESOURCES@NEWJFDB b
         where a.resources_id = b.resources_id
           AND resources_name IN
               ('个性化定制', '自助报表', '即席查询', '我的应用', '应用商店')
         group by to_char(a.visit_DATE, 'YYYYMM')) G,
       
       (SELECT COUNT(*) DL_CNT, TO_CHAR(a.start_date, 'YYYYMM') START_DATE
          FROM VBAP.VBAP_TASK_INSTANCE@NEWJFDB A, VBAP.VBAP_TASK_DEFINITION@NEWJFDB B
         WHERE A.TASK_ID = B.TASK_ID
         GROUP BY TO_CHAR(a.start_date, 'YYYYMM')) E
 WHERE A.month_id = G.visit_DATE(+)
   AND A.month_id = E.START_DATE(+)

 ORDER BY month_id desc;

 VBAP.pure_log_enter
 VBAP.pure_log_visit
 VBAP.VBAP_TASK_INSTANCE
 vbap.PURE_USER_ROLE
 vbap.PURE_ROLE