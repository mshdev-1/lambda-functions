import os
import sys
import json
import pandas as pd


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
            "ORDER BY calc_cp_code, content_title, platform;".format(
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
    print("type cp list ", type(cp_list))
    print(" cp code ", cp_id)

    ret_cp = None

    for cp_info in cp_list:
        if cp_info["cp_code_total"] == cp_id:
            ret_cp = cp_info
            break

    return ret_cp


# 상세내역 시트 컬럼명 가져오기
def get_detail_columns_name(locale):
    print("current find by locale", locale)
    report_lang = None
    with open("report_lang.json", "r") as lang:
        report_lang = json.load(lang)

    # 1. get cp info by cp_code
    # cp_info = get_cp_info_by_id(cp_code, list_cp)

    # check cp_info
    # locale, get_detail_columns_name
    print("current find by locale-2", locale)
    ret_list = None
    if locale == "중국":
        ret_list = report_lang["cn"]["detail_columns"]
    else:
        ret_list = report_lang["kr"]["detail_columns"]

    print("current columns names :", ret_list)

    return ret_list


# 정산서 요약 시트 데이터 생성
def generate_calc_summary_data(cp_info, list_sales):
    # get cp info - 유형(개인/사업자), 면세 유형(3가지), 언어(한글,한자), 정산금 선/후차감 구분
    cp_code = cp_info[1]
    cp_biz_type = cp_info[5]
    cp_tax_type = cp_info[10]
    cp_calc_lang = cp_info[9]
    cp_deduction_type = cp_info[11]

    print("current cp sales :", list_sales)
    # 3. 선급내역 : 이 항목은 추가 보강 필요
    # 2. 정산내역
    #   - 정산금 선/후차감 컬럼수 > "선차감" = 5개, "후차감" = 4개
    #   - "선차감" : 2,3,4 컬럼 내용 공란 처리
    #   - "정산금"
    #   - 작품별 정산금액 계산,

    # 1. 정산안내
    #   - 정산금 : 2.정산내역 > 정산금의 합계금액
    #   - 컬럼명은 : 과세여부(4가지)유형에 따라 달라짐
    #   - 과세여부 : 정산서 유형 구분에서 가져옴
    #   - 지급예정일 : 차월마지막일

    # 0. 시트 타이틀


# 2.정산내역
def proc_summary_detail(list_sale, cp_code, cp_calc_lang, cp_biz_type, cp_tax_type):
    return None


# 1. 정산안내
def proc_summary_total(list_sale, cp_code, cp_calc_lang, cp_deduction_type):
    return None
