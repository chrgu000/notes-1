create or replace procedure fuliankun_2I_charge_deal_begin(deal_date date) is
  PRAGMA AUTONOMOUS_TRANSACTION;
begin

  execute immediate 'truncate table FULIANKUN_2I_CHARGE_TMP1';
  insert into FULIANKUN_2I_CHARGE_TMP1 value
  (

    SELECT t.user_id,
      T.DEVICE_NUMBER,
      T.OPER_DATE,
      T.PAY_CHARGE,
      T.PAY_CHANNEL_CODE
    FROM GXUWCF2C.DM_D_RPT_YW_PAY_USER T
    WHERE 1=1
      and to_char(t.oper_date, 'yyyymmdd') = to_char(deal_date, 'yyyymmdd')
      and T.PAY_CHANNEL_CODE NOT IN (SELECT * FROM FULK_JFBM)
      AND ((T.PAY_CHANNEL_CODE NOT IN ('100015', '100016') AND T.PAY_CHARGE > 0) OR (T.PAY_CHANNEL_CODE IN ('100015', '100016')))
      and (t.pay_channel_name is null or t.pay_channel_name not like '%��%' )
  );
  commit;

  execute immediate 'truncate table FULIANKUN_2I_CHARGE_TMP2';
  insert into FULIANKUN_2I_CHARGE_TMP2 value
  (

    select t.user_id,
      t.DEVICE_NUMBER,
      sum(t.pay_charge) as total_pay_charge,
      min(t.oper_date) as oper_date,
      sum(case
          when t.PAY_CHANNEL_CODE IN ('100015', '100016') then to_number(t.pay_charge)
          else 0
        end) as pay_charge_number1,

      min(case
          when t.PAY_CHANNEL_CODE IN ('100015', '100016') then t.oper_date
          else to_date('20991231', 'yyyymmdd')
          end) as OPER_DATE1,

      sum(case
          when t.PAY_CHANNEL_CODE not IN ('100015', '100016') then to_number(t.pay_charge)
          else 0
      end) as pay_charge_number2,

      min(case
          when t.PAY_CHANNEL_CODE not IN ('100015', '100016') then t.oper_date
          else to_date('20991231', 'yyyymmdd')
      end) as OPER_DATE2

    from FULIANKUN_2I_CHARGE_TMP1 t
    where 1 = 1
    GROUP BY t.DEVICE_NUMBER, t.user_id

  );
  commit;

end;


create or replace procedure fuliankun_2I_charge_deal (deal_date in date, insert_number out number,
                  update_number out  number, invalid_number out  number) as
  /* ���ڼ���洢���ճ�ֵ������������� */
  user_id              number(16);
  DEVICE_NUMBER        number(16);
  first_charge_number  number(16);
  first_charge_date    date;
  second_charge_number number(16);
  second_charge_date   date;
  third_charge_number  number(16);
  third_charge_date    date;
  total_charge_number  number(16);

  /*�洢���ճ�ֵ�嵥������ȡ������*/
  cursor deal_charge_cursor(user_id1 varchar2, device_number1 varchar2) is
    select s.*
    from fuliankun_2I_number_charge s
    where s.user_id = user_id1
      and s.DEVICE_NUMBER = DEVICE_NUMBER1;
  charge_record fuliankun_2I_number_charge%ROWTYPE;

  /* �洢ÿ����Ч��ֵ������(����10015��10016) */
  cursor charge_cursor_tmp11(user_id1 varchar2, device_number1 varchar2) is
    select s.*
    from fuliankun_2i_charge_tmp1 s
    where s.user_id = user_id1
      and s.DEVICE_NUMBER = DEVICE_NUMBER1
      and pay_channel_code not in ('100015', '100016')
    order by s.oper_date asc, s.pay_channel_code asc, s.pay_charge desc;
  charge_record_tmp11 fuliankun_2i_charge_tmp1%ROWTYPE;

  /* �洢ÿ����Ч��ֵ������(��10015��10016) */

  cursor charge_cursor_tmp12(user_id1 varchar2, device_number1 varchar2) is
    select s.*
      from fuliankun_2i_charge_tmp1 s
    where s.user_id = user_id1
      and s.DEVICE_NUMBER = DEVICE_NUMBER1
      order by s.oper_date asc, s.pay_channel_code asc, s.pay_charge desc;
  charge_record_tmp12 fuliankun_2i_charge_tmp1%ROWTYPE;

  /*�洢ÿ����Ч��ֵ�����ӹ��������*/
  type deal_charge_cursor_tmp2 is ref cursor;
  charge_cursor_tmp2 deal_charge_cursor_tmp2;
  charge_record_tmp2 fuliankun_2i_charge_tmp2%ROWTYPE;

  /* �嵥������ʱ���� */
  first_charge_number_tmp3  number(16);
  first_charge_date_tmp3    date;
  second_charge_number_tmp3 number(16);
  second_charge_date_tmp3   date;
  third_charge_number_tmp3  number(16);
  third_charge_date_tmp3    date;
  total_charge_number_tmp3  number(16);

  /* ��ʱ�������� */
  loop_number1            number;
  loop_number2            number;
  pay_charge_number1_tmp2 number(16); /* '100015', '100016'�ɷѱ����ֵ����ܺ� */
  pay_charge_number2_tmp2 number(16); /* ��'100015', '100016'�ɷѱ����ֵ����ܺ� */

  charge_number_tmp1    number(16);
  charge_date_tmp1      date;
  pay_channel_code_tmp1 varchar2(64);
  charge_tmp2           number(16);
  calculate_flag        number;
  insert_flag           number;
  valid_charge_times    number;
  last_charge_time      date;
  last_charge_number    number;

begin
  dbms_output.enable(null);
  fuliankun_2I_charge_deal_begin(deal_date);
  insert_number := 0;
  update_number := 0;
  invalid_number := 0;
  ----------------------------------------------------------------------2������ÿ�����ݼӹ�
  open charge_cursor_tmp2 for
    select * from fuliankun_2i_charge_tmp2;
  loop_number1 := 0;
--  dbms_output.put_line('|--Begin loop table fuliankun_2i_charge_tmp2');
  loop
    /* ������ʼ�� */
    user_id                 := null;
    DEVICE_NUMBER           := null;
    first_charge_number     := null;
    first_charge_date       := null;
    second_charge_number    := null;
    second_charge_date      := null;
    third_charge_number     := null;
    third_charge_date       := null;
    total_charge_number     := null;
    pay_charge_number1_tmp2 := null;
    pay_charge_number2_tmp2 := null;

      /* �嵥������ʱ���� */
    first_charge_number_tmp3  := null;
    first_charge_date_tmp3    := null;
    second_charge_number_tmp3 := null;
    second_charge_date_tmp3   := null;
    third_charge_number_tmp3  := null;
    third_charge_date_tmp3    := null;
    total_charge_number_tmp3  := null;
    insert_flag               := 0;

    valid_charge_times        := 0;
    last_charge_time          := null;
    last_charge_number        := null;

/*    dbms_output.put_line('');*/

    FETCH charge_cursor_tmp2 into charge_record_tmp2;
    if charge_cursor_tmp2%NOTFOUND then
/*      dbms_output.put_line('|--End loop table fuliankun_2i_charge_tmp2 for find end, loop_number1=' ||
                loop_number1); */
      exit;
      else
      loop_number1        := loop_number1 + 1;
      user_id             := charge_record_tmp2.user_id;
      DEVICE_NUMBER       := charge_record_tmp2.DEVICE_NUMBER;
      total_charge_number := charge_record_tmp2.total_pay_charge;


/*      dbms_output.put_line('  |--table fuliankun_2i_charge_tmp2 , user_id=' ||
                user_id || ', DEVICE_NUMBER=' || DEVICE_NUMBER); */
      if total_charge_number = 0 then
/*        dbms_output.put_line('    |--table fuliankun_2i_charge_tmp2 , user_id=' ||
                  user_id || ', DEVICE_NUMBER=' ||
                  DEVICE_NUMBER || ',total_charge_number=0, continue'); */
        invalid_number := invalid_number + 1;
        continue;
      else
        pay_charge_number1_tmp2 := charge_record_tmp2.pay_charge_number1;
        pay_charge_number2_tmp2 := charge_record_tmp2.pay_charge_number2;
        loop_number2            := 0;

/*        dbms_output.put_line('    |--Begin loop table fuliankun_2i_charge_tmp2 user_id=' ||
                  user_id || ', DEVICE_NUMBER=' || DEVICE_NUMBER || ',total_charge_number=' ||
                  total_charge_number || ',pay_charge_number1_tmp2=' || pay_charge_number1_tmp2); */

        /* �����ֵ�嵥�� '100015', '100016' ��ֵ����ܶ�Ϊ0 */
        if pay_charge_number1_tmp2 = 0 then
          open charge_cursor_tmp11(user_id, DEVICE_NUMBER);

          loop
            FETCH charge_cursor_tmp11 into charge_record_tmp11;
              if charge_cursor_tmp11%NOTFOUND then
/*                dbms_output.put_line('    |--End loop table fuliankun_2i_charge_tmp1 (notin [100015, 100016]) for find end, loop_number2=' ||
                          loop_number2 || ',DEVICE_NUMBER= ' || DEVICE_NUMBER || ',user_id= ' || user_id); */
                exit;
              else
                loop_number2 := loop_number2 + 1;
                valid_charge_times := valid_charge_times + 1;
                last_charge_time := charge_record_tmp11.oper_date;
                last_charge_number := charge_record_tmp11.pay_charge;

/*                dbms_output.put_line('      |--loop table fuliankun_2i_charge_tmp1 end, loop_number2=' ||
                          loop_number2 || ',DEVICE_NUMBER= ' || charge_record_tmp11.DEVICE_NUMBER ||
                          ',user_id= ' || charge_record_tmp11.user_id); */
                charge_tmp2 := charge_record_tmp11.pay_charge;
                if charge_tmp2 is null then
                  charge_tmp2 := 0;
                end if;

                if loop_number2 = 1 then
                  first_charge_number := charge_tmp2;
                  first_charge_date   := charge_record_tmp11.oper_date;

                elsif loop_number2 = 2 then
                  second_charge_number := charge_tmp2;
                  second_charge_date   := charge_record_tmp11.oper_date;
                elsif loop_number2 = 3 then
                  third_charge_number := charge_tmp2;
                  third_charge_date   := charge_record_tmp11.oper_date;
                else
/*                  dbms_output.put_line('    |--End loop table fuliankun_2i_charge_tmp1 (notin [100015, 100016]), loop_number2=' ||
                            loop_number2 || ' for over 3 ,DEVICE_NUMBER= ' || charge_record_tmp11.DEVICE_NUMBER ||
                            ',user_id= ' || charge_record_tmp11.user_id);  */
                  continue;
                end if;
              end if;
          end loop;
          close charge_cursor_tmp11;
        else
        /* �����ֵ�嵥�� '100015', '100016' ��ֵ����ܶΪ0 */
          open charge_cursor_tmp12(user_id, DEVICE_NUMBER);

          charge_number_tmp1 := null;
          charge_date_tmp1   := null;
          calculate_flag     := 0;

          loop
            FETCH charge_cursor_tmp12 into charge_record_tmp12;
            if charge_cursor_tmp12%NOTFOUND then
/*              dbms_output.put_line('    |--End loop table fuliankun_2i_charge_tmp1 (in [100015, 100016]) for find end, loop_number2=' ||
                        loop_number2 || ',DEVICE_NUMBER= ' || DEVICE_NUMBER || ', user_id= ' || user_id ); */
              if calculate_flag = 1 then
                if charge_number_tmp1 != 0 then
                    /* ���һ�γ�ֵ��¼Ϊ100015�� 100016, �ҳ�ֵ�ܺͲ�Ϊ0����Ч��ֵ����+1���������һ�γ�ֵ��Ϣ  */
                  valid_charge_times := valid_charge_times + 1;
                  last_charge_time := charge_date_tmp1;
                  last_charge_number := charge_number_tmp1;

                  if first_charge_number is null then
                    first_charge_number := charge_number_tmp1;
                    first_charge_date   := charge_date_tmp1;
                  elsif second_charge_number is null then
                    second_charge_number := charge_number_tmp1;
                    second_charge_date   := charge_date_tmp1;
                  elsif third_charge_number is null then
                    third_charge_number := charge_number_tmp1;
                    third_charge_date   := charge_date_tmp1;
                  end if;
                end if;
                calculate_flag := 0;
              end if;
              exit;
            else
              loop_number2 := loop_number2 + 1;
/*              dbms_output.put_line('      |--loop table fuliankun_2i_charge_tmp1 end, loop_number2=' ||
                        loop_number2 || ',DEVICE_NUMBER= ' || charge_record_tmp12.DEVICE_NUMBER ||
                        ',user_id= ' || charge_record_tmp12.user_id || ',PAY_CHANNEL_CODE=' ||
                        charge_record_tmp12.PAY_CHANNEL_CODE || ', pay_charge=' || charge_record_tmp12.pay_charge); */
              pay_channel_code_tmp1 := charge_record_tmp12.PAY_CHANNEL_CODE;

              charge_tmp2 := charge_record_tmp12.pay_charge;
              if charge_tmp2 is null then
                charge_tmp2 := 0;
              end if;
              if pay_channel_code_tmp1 in ('100015', '100016') then
                if charge_number_tmp1 is null then
                  charge_number_tmp1 := charge_tmp2;
                  charge_date_tmp1   := charge_record_tmp12.oper_date;
                  calculate_flag     := 1;
                else
                  charge_number_tmp1 := charge_number_tmp1 + charge_tmp2;
                end if;
              else
                valid_charge_times := valid_charge_times + 1;
                last_charge_time := charge_record_tmp12.oper_date;
                last_charge_number := charge_record_tmp12.pay_charge;

                if calculate_flag = 1 and charge_number_tmp1 != 0 then
                  valid_charge_times := valid_charge_times + 1;
                  if first_charge_number is null then
                    first_charge_number  := charge_number_tmp1;
                    first_charge_date    := charge_date_tmp1;
                    second_charge_number := charge_tmp2;
                    second_charge_date   := charge_record_tmp12.oper_date;
                  elsif second_charge_number is null then
                    second_charge_number := charge_number_tmp1;
                    second_charge_date   := charge_date_tmp1;
                    third_charge_number  := charge_tmp2;
                    third_charge_date    := charge_record_tmp12.oper_date;
                  elsif third_charge_number is null then
                    third_charge_number := charge_number_tmp1;
                    third_charge_date   := charge_date_tmp1;
                  end if;
                else
                  if first_charge_number is null then
                    first_charge_number := charge_tmp2;
                    first_charge_date   := charge_record_tmp12.oper_date;
                  elsif second_charge_number is null then
                    second_charge_number := charge_tmp2;
                    second_charge_date   := charge_record_tmp12.oper_date;
                  elsif third_charge_number is null then
                    third_charge_number := charge_tmp2;
                    third_charge_date   := charge_record_tmp12.oper_date;
                  end if;
                end if;
                charge_number_tmp1 := null;
                charge_date_tmp1   := null;
                calculate_flag     := 0;
              end if;
            end if;
          end loop;
          close charge_cursor_tmp12;
        end if;
      end if;

/*      dbms_output.put_line('  |--Deal deal_charge_cursor_tmp for , loop_number2=' ||
                loop_number2 || ': DEVICE_NUMBER= ' ||
                DEVICE_NUMBER || ', user_id= ' || user_id || ': first_charge_number= ' ||
                first_charge_number || ', first_charge_date =' ||
                to_char(first_charge_date, 'yyyymmdd hh24:mi:ss') || ', second_charge_number= ' || second_charge_number ||
                ', second_charge_date =' || to_char(second_charge_date, 'yyyymmdd hh24:mi:ss') || ', third_charge_number= ' ||
                third_charge_number || ', third_charge_date =' || to_char(third_charge_date, 'yyyymmdd hh24:mi:ss') ||
                ', total_charge_number= ' || total_charge_number);


      dbms_output.put_line('    |--Begin deal table fuliankun_2I_number_charge .'|| ', DEVICE_NUMBER= ' || DEVICE_NUMBER ||
                ',user_id= ' || user_id);
*/
      open deal_charge_cursor(user_id, DEVICE_NUMBER);
      loop
        FETCH deal_charge_cursor into charge_record;
        if deal_charge_cursor%NOTFOUND then   /* �ۼƼ������δ���иú���ĳ�ֵ��¼����Ҫ�����¼�¼ */
/*          dbms_output.put_line('    |--END deal table fuliankun_2I_number_charge no record'|| ', DEVICE_NUMBER= ' || DEVICE_NUMBER ||
                    ',user_id= ' || user_id);*/
          first_charge_number_tmp3 := first_charge_number;
          first_charge_date_tmp3 := first_charge_date;
          second_charge_number_tmp3 := second_charge_number;
          second_charge_date_tmp3 := second_charge_date;
          third_charge_number_tmp3 := third_charge_number;
          third_charge_date_tmp3 := third_charge_date;
          total_charge_number_tmp3 := total_charge_number;
          insert_flag := 1;
          insert_number := insert_number + 1;
          exit;
        else /* �ۼƼ������δ���иú���ĳ�ֵ��¼������Ҫ�����¼�¼���Լ�¼���и��¼��� */
          update_number := update_number + 1;

          if first_charge_number is null then
            first_charge_number_tmp3 := charge_record.first_charge_number;
            first_charge_date_tmp3 := charge_record.first_charge_date;
            second_charge_number_tmp3 := charge_record.second_charge_number;
            second_charge_date_tmp3 := charge_record.second_charge_date;
            third_charge_number_tmp3 := charge_record.third_charge_number;
            third_charge_date_tmp3 := charge_record.third_charge_date;
            total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;

          elsif second_charge_number is null then   /* �¼�¼ֻ���׳� */
            if charge_record.first_charge_number is null then   /* ���¼���׳� */
              first_charge_number_tmp3 := first_charge_number;
              first_charge_date_tmp3 := first_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.second_charge_number is null then /* ���¼ֻ���׳� */
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := first_charge_number;
              second_charge_date_tmp3 := first_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.third_charge_number is null then /* ���¼�����׳估���� */
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := first_charge_number;
              third_charge_date_tmp3 := first_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            else  /* ���¼�����׳䡢���䡢���� */
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := charge_record.third_charge_number;
              third_charge_date_tmp3 := charge_record.third_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            end if;

          elsif third_charge_number is null then
            if charge_record.first_charge_number is null then
              first_charge_number_tmp3 := first_charge_number;
              first_charge_date_tmp3 := first_charge_date;
              second_charge_number_tmp3 := second_charge_number;
              second_charge_date_tmp3 := second_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.second_charge_number is null then
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := first_charge_number;
              second_charge_date_tmp3 := first_charge_date;
              third_charge_number_tmp3 := second_charge_number;
              third_charge_date_tmp3 := second_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.third_charge_number is null then
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := first_charge_number;
              third_charge_date_tmp3 := first_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            else
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := charge_record.third_charge_number;
              third_charge_date_tmp3 := charge_record.third_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            end if;
          else
            if charge_record.first_charge_number is null then
              first_charge_number_tmp3 := first_charge_number;
              first_charge_date_tmp3 := first_charge_date;
              second_charge_number_tmp3 := second_charge_number;
              second_charge_date_tmp3 := second_charge_date;
              third_charge_number_tmp3 := third_charge_number;
              third_charge_date_tmp3 := third_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.second_charge_number is null then
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := first_charge_number;
              second_charge_date_tmp3 := first_charge_date;
              third_charge_number_tmp3 := second_charge_number;
              third_charge_date_tmp3 := second_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            elsif charge_record.third_charge_number is null then
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := first_charge_number;
              third_charge_date_tmp3 := first_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            else
              first_charge_number_tmp3 := charge_record.first_charge_number;
              first_charge_date_tmp3 := charge_record.first_charge_date;
              second_charge_number_tmp3 := charge_record.second_charge_number;
              second_charge_date_tmp3 := charge_record.second_charge_date;
              third_charge_number_tmp3 := charge_record.third_charge_number;
              third_charge_date_tmp3 := charge_record.third_charge_date;
              total_charge_number_tmp3 := total_charge_number + charge_record.total_charge_number;
            end if;

          end if;
/*          dbms_output.put_line('    |--END deal table fuliankun_2I_number_charge have record'|| ', DEVICE_NUMBER= ' || DEVICE_NUMBER ||
                    ',user_id= ' || user_id); */
          exit;

        end if;
      end loop;
      close deal_charge_cursor;
/*      dbms_output.put_line('  |--Deal deal_charge_cursor_tmp for , loop_number2=' ||
                loop_number2 || ', insert_flag ='||insert_flag||' DEVICE_NUMBER= ' ||
                DEVICE_NUMBER || ', user_id= ' || user_id || ': first_charge_number_tmp3= ' ||
                first_charge_number_tmp3 || ', first_charge_date_tmp3 =' ||
                to_char(first_charge_date_tmp3, 'yyyymmdd hh24:mi:ss') || ', second_charge_number_tmp3= ' || second_charge_number_tmp3 ||
                ', second_charge_date_tmp3 =' || to_char(second_charge_date_tmp3, 'yyyymmdd hh24:mi:ss') || ', third_charge_number_tmp3= ' ||
                third_charge_number_tmp3 || ', third_charge_date_tmp3 =' || to_char(third_charge_date_tmp3, 'yyyymmdd hh24:mi:ss') ||
                ', total_charge_number_tmp3= ' || total_charge_number_tmp3); */

      fuliankun_2I_charge_deal_end(insert_flag, user_id, DEVICE_NUMBER, first_charge_number_tmp3, first_charge_date_tmp3, second_charge_number_tmp3,
              second_charge_date_tmp3,third_charge_number_tmp3, third_charge_date_tmp3, total_charge_number_tmp3, deal_date,
              valid_charge_times, last_charge_time, last_charge_number);

    end if;


  end loop;

  close charge_cursor_tmp2;

end;