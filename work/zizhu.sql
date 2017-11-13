SELECT DAY_ID, 
LOGIN_USER_CNT,---登录用户数 
DL_CNT, ---导出文件数
LOGIN_CNT, --登陆次数
mkfw ----模块访问次数
  FROM (SELECT COUNT(*) LOGIN_CNT,
               COUNT(DISTINCT USER_ID) LOGIN_USER_CNT,
               TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') DAY_ID
          FROM VBAP.pure_log_enter a ----结果表名
        ---WHERE TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') = '20160629'
         GROUP BY TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD')) A,
       (SELECT COUNT(*) mkfw, to_char(a.visit_DATE, 'YYYYMMDD') visit_DATE
          FROM VBAP.pure_log_visit a, VBAP.PURE_RESOURCES b
         where a.resources_id = b.resources_id
           AND resources_name IN
               ('个性化定制', '自助报表', '即席查询', '我的应用', '应用商店')
         group by to_char(a.visit_DATE, 'YYYYMMDD')) G,
       
       (SELECT COUNT(*) DL_CNT, TO_CHAR(a.start_date, 'YYYYMMDD') START_DATE
          FROM VBAP.VBAP_TASK_INSTANCE A, VBAP.VBAP_TASK_DEFINITION B
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
          FROM VBAP.pure_log_enter a ----结果表名
        ---WHERE TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD') = '20160629'
         GROUP BY TO_CHAR(A.LOGIN_DATE, 'YYYYMMDD')) A,
       (SELECT COUNT(*) mkfw, to_char(a.visit_DATE, 'YYYYMMDD') visit_DATE
          FROM VBAP.pure_log_visit a, VBAP.PURE_RESOURCES b
         where a.resources_id = b.resources_id
           AND resources_name IN
               ('个性化定制', '自助报表', '即席查询', '我的应用', '应用商店')
         group by to_char(a.visit_DATE, 'YYYYMMDD')) G,
       
       (SELECT COUNT(*) DL_CNT, TO_CHAR(a.start_date, 'YYYYMMDD') START_DATE
          FROM VBAP.VBAP_TASK_INSTANCE A, VBAP.VBAP_TASK_DEFINITION B
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