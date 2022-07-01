import os
import sys
import json
import pandas as pd
import math

# for date
import calendar
from datetime import datetime
from datetime import timedelta
from dateutil import relativedelta


def func_name():
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name


# %%
# 월별 매출 데이터 가져오기 from DB
def get_calc_data_from_db(table_name, calc_month, from_source="s3"):
    try:
        if from_source == "s3":
            import layers.dbManager.python.dbManager as DBManager
        else:
            sys.path.append("../layers")
            from layers.dbManager.python import dbManager as DBManager

        dbMgr = DBManager.DBManager()
        db_engine = dbMgr.get_engine()
        # df.to_sql(name=table_name, con=db_engine, if_exists="replace", index=True)
        # cp_code_total
        sql = (
            "SELECT * FROM public.\"{0}\" Where calc_month='{1}' "
            # "ORDER BY calc_cp_name, content_title, platform;".format(
            # "ORDER BY calc_cp_code, content_title, platform;".format(
            "ORDER BY calc_cp_code, content_series_code, platform;".format(
                table_name, calc_month
            )
        )
        result = db_engine.execute(sql)
        result_set = result.fetchall()

        return result_set

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


# %%
# cp 정산서 정보 가져오기 from DB
def get_calc_cp_all_info(from_source="s3"):
    try:
        if from_source == "s3":
            import layers.dbManager.python.dbManager as DBManager
        else:
            sys.path.append("../layers")
            from layers.dbManager.python import dbManager as DBManager

        dbMgr = DBManager.DBManager()
        db_engine = dbMgr.get_engine()
        # df.to_sql(name=table_name, con=db_engine, if_exists="replace", index=True)
        table_name = "CalcCp"
        sql = (
            'SELECT * FROM public."{0}" '.format(table_name)
            # "ORDER BY calc_cp_name, content_title, platform;"
        )
        result = db_engine.execute(sql)
        result_set = result.fetchall()

        global list_calc_cp_infos
        list_calc_cp_infos = result_set

        return result_set

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


# %%
# 정산서 생성을 위한 cp 정보 가져오기 by cp_id
def get_cp_info_by_id(cp_id, cp_list):
    ret_cp = None

    for cp_info in cp_list:
        if cp_info["cp_code_total"] == cp_id:
            ret_cp = cp_info
            break

    return ret_cp


# local 변수명 가져오기
def get_locale_contants(locale):
    report_lang = None
    with open("report_lang.json", "r") as lang:
        report_lang = json.load(lang)

    ret_list = None
    if locale == "중국":
        ret_list = report_lang["cn"]
    else:
        ret_list = report_lang["kr"]

    return ret_list


# 상세내역 시트 컬럼명 가져오기
def get_detail_columns_name(locale):
    report_lang = None
    with open("report_lang.json", "r") as lang:
        report_lang = json.load(lang)

    # 1. get cp info by cp_code
    # cp_info = get_cp_info_by_id(cp_code, list_cp)

    # check cp_info
    # locale, get_detail_columns_name
    ret_list = None
    if locale == "중국":
        ret_list = report_lang["cn"]["detail_columns"]
    else:
        ret_list = report_lang["kr"]["detail_columns"]

    # print("current columns names :", ret_list)

    return ret_list


# 정산서 요약 시트 데이터 생성
def generate_calc_summary_data(cp_info, list_sale, calc_month):
    # get cp info - 유형(개인/사업자), 면세 유형(3가지), 언어(한글,한자), 정산금 선/후차감 구분
    # 42	CPDV00042_01	CPDV00042	동아		사업자	Bi000042CO	동아	총매출	한국	면세	후차감	O	Bi013	0			1	1
    cp_code = cp_info[1]
    cp_biz_type = cp_info[5]  # 사업자 유형
    cp_tax_type = cp_info[10]  # 세금 유형
    cp_calc_lang = cp_info[9]  # 언어
    cp_deduction_type = cp_info[11]  # 정산금 선/후차감
    cp_calc_std = cp_info[8]  # 정산기준 (총매출/순매출)

    # if cp_info[3] == "동아":
    #     print(f"\n\n동아 sales\n", list_sale)

    # final return
    dic_final = {"title": None, "total": None, "detail": None, "prepayment": None}

    # print("current cp sales :", list_sale)
    locale_contants = get_locale_contants(cp_calc_lang)
    # 3. 선급내역 : 이 항목은 추가 보강 필요
    # 2. 정산내역
    df_summary_detail = proc_summary_detail(
        list_sale,
        cp_code,
        cp_calc_lang,
        cp_biz_type,
        cp_deduction_type,
        locale_contants,
    )
    dic_final["detail_title"] = locale_contants["summary_2_detail_title"]
    dic_final["detail"] = df_summary_detail

    # 1. 정산안내
    df_summary_total = proc_summary_total(
        df_summary_detail, cp_biz_type, cp_tax_type, locale_contants, calc_month
    )
    dic_final["total_title"] = locale_contants["summary_1_total_title"]
    dic_final["total"] = df_summary_total

    # 맨하단 안내 문구
    list_summary_common_guide = proc_summary_common_guide(
        cp_tax_type, cp_biz_type, locale_contants
    )
    dic_final["common_guide"] = list_summary_common_guide

    # 0. 요약 타이틀
    str_title_format = locale_contants["summary_title"]
    df_summary_title = proc_summary_title(calc_month, str_title_format)

    dic_final["df_title"] = df_summary_title

    # sheet title
    dic_final["sheet_summary"] = locale_contants["sheet_summary"]
    dic_final["sheet_detail"] = locale_contants["sheet_detail"]

    return dic_final


# 2.정산내역
#   - 정산금 선/후차감 컬럼수 > "선차감" = 5개, "후차감" = 4개
#   - "선차감" : 2,3,4 컬럼 내용 공란 처리
#   - "정산금"
#   - 작품별 정산금액 계산,
def proc_summary_detail(
    list_sale, cp_code, cp_calc_lang, cp_biz_type, cp_deduction_type, locale_contants
):
    # key_f = lambda x: x[18]
    # for key, group in itertools.groupby(list_sale, key_f):
    #     print("key:\n" + key + ": " + str(list(group)))

    # 1. 정산금 선/후차감
    #   - 선차감 : 5개, 2~4 컬럼 공란 처리
    #       > 내용	매출(정산대상)(A)	차감대상(B)	매출-차감대상(A-B)	정산율(C)	정산금((A-B)*C)
    #   - 후차감 : 4개
    #       > 내용	매출(정산대상)(A)	정산금(B)	선급 차감(C)	지급 예정(B-C)

    # print("정산금 선/후차감:", cp_deduction_type)

    #  [4, '아카이브팩토리', '2022-02', '네이버 만화', '그것은 어느날', '참꽃마리', 'archivefactory', 70.0, 28000.0, 3.0, 600.0, None, None, None, 28600.0, 0.6, 17160.0, '그것은 어느날 ', 'SECM00003', 'CPDV00021_01']
    # 작품명 - 4, 정산기준 - 21
    filter_list_sale = []
    for item in list_sale:
        if "A선수수익 정산" != item[21]:
            filter_list_sale.append(item)

    # test - 동아
    # if cp_code == "CPDV00042_01":
    #     print(f"\n\n\b동아 상세내역 filter after\n{filter_list_sale}\n\n")

    # print("\n\n\n*** filter_list_sale \n ", filter_list_sale)

    now_series_code = ""  # index-18
    now_series_title = ""
    now_series_count = 0
    now_series_sales_sum = 0
    now_series_calc_sum = 0
    now_series_prepayment_deduction_sum = 0

    final_list = []
    for item in filter_list_sale:
        if now_series_code != item[18]:

            if now_series_count > 0:
                final_list.append(
                    [
                        now_series_title,
                        now_series_sales_sum,
                        now_series_calc_sum,
                        now_series_prepayment_deduction_sum,
                    ]
                )

                # init
                now_series_count = 1
                now_series_sales_sum = item[14]
                now_series_calc_sum = item[16]

                now_series_code = item[18]
            else:
                now_series_count += 1
                now_series_sales_sum += item[14]
                now_series_calc_sum += item[16]

            now_series_title = item[4]
            #
        else:
            now_series_count += 1
            now_series_sales_sum += item[14]
            now_series_calc_sum += item[16]

    # end loop > append
    if now_series_count > 0:
        final_list.append(
            [
                now_series_title,
                now_series_sales_sum,
                now_series_calc_sum,
                now_series_prepayment_deduction_sum,
            ]
        )

    # 합계 계산 및 추가
    sum_sales_sum = 0
    sum_calc_sum = 0
    sum_deduction_sum = 0
    for item in final_list:
        sum_sales_sum += item[1]
        sum_calc_sum += item[2]
        sum_deduction_sum += item[3]

    # get 'sum' contant
    str_sum = locale_contants["sum"]
    if len(final_list) > 0:
        final_list.append([str_sum, sum_sales_sum, sum_calc_sum, sum_deduction_sum])

    # print("\n\n\n*** calc final \n ", final_list)

    # df_detail = pd.DataFrame(data_frame.get("list"), columns=data_frame.get("headers"))
    df_summary_detail = None
    if cp_deduction_type == "후차감":  # 4개
        # 내용	매출(정산대상)(A)	정산금(B)	선급 차감(C)	지급 예정(B-C)
        # 정산기준 - "A선수수익 정산" 제외
        # @To-do : 선급 차감 내역 추가 필요(선급 차감 급액 별도 집계, 정산금 계산식 수정 필요
        detail_headers = locale_contants["summary_2_detail_5_headers"]
        detail_list = []
        for item in final_list:
            detail_list.append([item[0], item[1], item[2], item[3], item[2]])

        df_summary_detail = pd.DataFrame(detail_list, columns=detail_headers)
        # df_summary_detail.style.set_caption("Hello World")
        # df_summary_detail = df_summary_detail.style.set_caption("Hello World")

    else:  # 5개
        # 내용	매출(정산대상)(A)	차감대상(B)	매출-차감대상(A-B)	정산율(C)	정산금((A-B)*C)
        detail_headers = locale_contants["summary_2_detail_6_headers"]
        detail_list = []
        for item in final_list:
            detail_list.append([item[0], item[1], "", "", "", item[2]])

        df_summary_detail = pd.DataFrame(detail_list, columns=detail_headers)

    # print("# >> detail DataFrame : ", df_summary_detail)

    return df_summary_detail


# 1. 정산안내
#   - 정산금 : 2.정산내역 > 정산금의 합계금액
#   - 컬럼명은 : 과세여부(4가지)유형에 따라 달라짐
#   - 과세여부 : 정산서 유형 구분에서 가져옴
#   - 지급예정일 : 차월마지막일


def proc_summary_total(
    df_summary_detail, cp_biz_type, cp_tax_type, locale_contants, calc_month
):
    total_headers = locale_contants["summary_1_tax_headers_personal"]
    if cp_biz_type == "사업자":
        total_headers = locale_contants["summary_1_tax_headers_company"]

    # logic
    # 1. 과세여부
    #   "면세" | "과세(부가세별도)" | "과세(부가세포함)"
    #   "원천징수" >> 사업자유형에 따라 계산식 분기
    # print(f"세금유형 : {cp_tax_type}")

    # 품목	공급가액	세액	합계	과세여부	지급예정일
    # 1. 품목
    content_title = calc_month + " " + locale_contants["calc"]
    # 2. 공급가액(사업자) | 정산금(개인) << "원천징수"인 경우에만 해당 >> 컬럼 헤드만 다르다
    amount_content = 0
    # 3. 세액
    tax_calc = 0
    # 4. 합계
    total_calc = 0

    # 정산내역 전체 rows, last cols
    # print(f"df_summary_detail : {df_summary_detail}")

    df_detail = df_summary_detail
    detail_row = len(df_detail)
    detail_col = len(df_detail.columns)
    amount_content = df_detail.iloc[detail_row - 1][detail_col - 1]
    # print(f" detail total sum : {amount_content}")

    # amount_content
    # IFS(F6="면세",0,F6="과세(부가세별도)",ROUNDDOWN(C6*0.1,0),F6="과세(부가세포함)",ROUNDDOWN(C6/1.1,0))

    if cp_tax_type == "면세":
        tax_calc = 0
        total_calc = round(amount_content - tax_calc)

    elif cp_tax_type == "과세(부가세포함)":
        tax_calc = rounddown(amount_content / 1.1)
        total_calc = amount_content + tax_calc

    elif cp_tax_type == "과세(부가세별도)":
        tax_calc = rounddown(amount_content * 0.1)
        total_calc = amount_content + tax_calc  # check

    elif cp_tax_type == "원천징수":
        # print(f"\n[Exception] - 세금유형:{cp_tax_type}, CP info:{df_summary_detail}")
        if cp_biz_type == "개인":
            if amount_content > 33400:
                tax_calc = rounddown(amount_content * 0.03) + rounddown(
                    amount_content * 0.003
                )
            else:
                tax_calc = 0

            total_calc = amount_content - tax_calc
        else:
            tax_calc = rounddown(amount_content * 0.1 * 100 / 110) + rounddown(
                amount_content * 0.1 * 100 / 110 * 0.1
            )
            total_calc = amount_content
            amount_content = total_calc - tax_calc

    # 지급예정일
    arr_date = calc_month.split("-")
    calc_year = arr_date[0]
    calc_month = arr_date[1]
    date_send = get_date_next_month_lastday(calc_year, calc_month)

    # print(f"\n\n\n 내용 : {content_title}")
    # print(f"amount_content : {amount_content}")
    # print(f"tax_calc : {tax_calc}")
    # print(f"total_calc : {total_calc}")
    # print(f"tax : {cp_tax_type}")
    # print(f"date : {date_send}")

    total_list = []
    total_list.append(
        [content_title, amount_content, tax_calc, total_calc, cp_tax_type, date_send]
    )

    df_summary_total = pd.DataFrame(total_list, columns=total_headers)
    # print(f"\n\ntotal_list:{total_list}")

    # # test
    # x = 77.77
    # prec = -1
    # ret = rounddown(x, prec, 0.1)
    # print(f"\n\n\n TEST rounddown : {x} >> {ret}")
    # ret2 = round(x)
    # print(f"\nTEST round default : {x} >> {ret2}")
    # ret3 = math.floor(x)
    # print(f"\nTEST math.floor default : {x} >> {ret3}")
    return df_summary_total


def proc_summary_common_guide(cp_tax_type, cp_type, locale_contants):
    ret_guide = locale_contants["summary_common_guide_tax_free"]

    if (
        cp_tax_type == "원천징수"
        or cp_tax_type == "과세(부가세포함)"
        or cp_tax_type == "과세(부가세별도)"
    ) and cp_type == "사업자":
        ret_guide = locale_contants["summary_common_guide_tax_include"]

    return ret_guide


def proc_summary_title(calc_month, str_title):
    arr_date = calc_month.split("-")
    calc_year = arr_date[0]
    calc_month = arr_date[1]

    title = str_title.format(calc_year, calc_month)
    df_title = pd.DataFrame({title: []})
    return df_title


# 지급예정일 구하기, 다음달 말일
def get_date_next_month_lastday(now_year, now_month):
    now_year = int(now_year)
    now_month = int(now_month)
    this_month = datetime(year=now_year, month=now_month, day=1).date()
    next_month = this_month + relativedelta.relativedelta(months=1)

    monthrange = calendar.monthrange(next_month.year, next_month.month)
    first_day = calendar.monthrange(next_month.year, next_month.month)[0]
    last_day = calendar.monthrange(next_month.year, next_month.month)[1]

    # print(f"next month: {next_month}")
    # print(f"month range: {monthrange}")
    # print(f"next year: {next_month.year}")
    # print(f"next month: {next_month.month}")
    # print(f"next last day: {last_day}")
    # (next_month.year, next_month.month, last_day)

    return "{0}/{1}/{2}".format(next_month.year, next_month.month, last_day)


# excel rounddown()
# def rounddown(x, prec=2, base=0.05):
# 좀더 검증이 필요한 함수
# def rounddown(x, prec=0, base=0.1):
#     return round(base * round(float(x) / base), prec)


def rounddown(x):
    return math.floor(x)
