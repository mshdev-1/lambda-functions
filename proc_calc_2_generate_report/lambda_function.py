import os
import sys
import json
import pandas as pd

# from io import BytesIO
import io
import boto3

import xlsxwriter
import data_handler

# for only aws lambda
# import dbManager as DBManager

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


def func_name():
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name


###### status ######
def finish_job_status(
    table_status, job_id, calc_month, working_data=-1, from_source="s3"
):
    try:
        if from_source == "s3":
            import layers.dbManager.python.dbManager as DBManager
        else:
            # sys.path.append("../layers")
            # from layers.dbManager.python import dbManager as DBManager
            return

        dbMgr = DBManager.DBManager()
        db_engine = dbMgr.get_engine()
        # table_status = "StatusCalcWork"

        sql = "Update  public.\"{0}\" set  flag_working={1}, date_end =now() where id= (select id from public.\"{0}\" Where job_id='{2}' and calc_month='{3}' Order by date_start desc limit 1);".format(
            table_status, working_data, job_id, calc_month
        )
        # print("sql -", sql)
        result = db_engine.execute(sql)
        return result

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return e


###### status ######


def create_excel_file(
    data_frame,
    dest_excel_file,
    p_sheet_name="sheet",
    calc_cp_info=None,
    calc_month=None,
):
    try:
        df_detail = pd.DataFrame(
            data_frame.get("list"), columns=data_frame.get("headers")
        )

        # create excel file
        # print("** create excel file : " + dest_excel_file)

        # create Excel file
        writer = pd.ExcelWriter(dest_excel_file, engine="xlsxwriter")

        # ** generate 정산서 요약 시트 데이터 생성 처리 **
        dic_summary = data_handler.generate_calc_summary_data(
            calc_cp_info, data_frame.get("list"), calc_month
        )

        # summary - 0. title
        df_summary_title = dic_summary["df_title"]

        # summary - 1. total
        df_summary_total_title = dic_summary["total_title"]
        df_summary_total = dic_summary["total"]

        # summary - 2. detail
        df_summary_detail_title = dic_summary["detail_title"]
        df_summary_detail = dic_summary["detail"]

        ###################################
        #   1. sheet - summary
        ###################################
        sheet_summary = dic_summary["sheet_summary"]

        # 0. 제목
        df_summary_title.to_excel(
            writer, sheet_name=sheet_summary, index=False, startcol=1, startrow=0
        )
        str_summary_title = list(df_summary_title.columns.values)[0]

        workbook = writer.book
        worksheet_summary = writer.sheets[sheet_summary]

        # cell format
        # worksheet_summary.set_column("B:G", 12)
        currency_format = workbook.add_format({"num_format": "#,##0"})
        # worksheet_summary.set_column("C:G", currency_format)
        worksheet_summary.hide_gridlines(2)
        worksheet_summary.set_column("A:A", 3)
        worksheet_summary.set_column("B:B", 35)
        worksheet_summary.set_column("C:G", 15, currency_format)
        worksheet_summary.set_row(0, 30)

        cell_title_format = workbook.add_format(
            {
                "bold": 1,
                "border": 0,
                "align": "center",
                "valign": "vcenter",
                "font_size": 14,
                # "fg_color": "yellow",
            }
        )
        worksheet_summary.merge_range("B1:G1", str_summary_title, cell_title_format)

        # 1.정산 안내
        worksheet_summary.write(3, 1, df_summary_total_title)
        df_summary_total.to_excel(
            writer, sheet_name=sheet_summary, index=False, startcol=1, startrow=4
        )

        # 2.정산내역
        worksheet_summary.write(11, 1, df_summary_detail_title)
        df_summary_detail.to_excel(
            writer, sheet_name=sheet_summary, index=False, startcol=1, startrow=12
        )

        # 맨하단 안내 문구
        cell_summary_common_guide_format = workbook.add_format({"font_size": 9})
        summary_common_guide_tax = dic_summary["common_guide"]

        # 시작 행 > 12 + detail row
        row_common_start = 12 + len(df_summary_detail) + 3
        for item in summary_common_guide_tax:
            worksheet_summary.write(
                row_common_start, 1, item, cell_summary_common_guide_format
            )
            row_common_start += 1

        ###################################
        #   2. sheet - detail
        ###################################
        # sheet = 상세내역
        # df_detail = df_detail.style.set_table_attributes(
        #     "style='display:inline'"
        # ).set_caption("Caption table")

        cp_calc_lang = calc_cp_info[9]  # 언어
        locale_contants = data_handler.get_locale_contants(cp_calc_lang)

        sheet_detail = dic_summary["sheet_detail"]

        # 최종 excel 생성 전, 불필요한 컬럼들 삭제 처리
        # 저작권자,저작권자 코드, 매출기준, 정산구분

        detail_headers = locale_contants["detail_columns"]
        detail_cp = detail_headers[1]
        detail_cp_code = detail_headers[19]
        detail_standard_sale = detail_headers[20]
        detail_standard_calc = detail_headers[21]
        df_detail = df_detail.drop(
            columns=[
                detail_cp,
                detail_cp_code,
                detail_standard_sale,
                detail_standard_calc,
            ]
        )

        # sheet 생성
        df_detail.to_excel(
            writer, sheet_name=sheet_detail, index=False, startcol=0, startrow=0
        )

        worksheet_detail = writer.sheets[sheet_detail]
        detail_cell_hedaer_format = workbook.add_format(
            {
                "bold": 1,
                "border": 1,
                "align": "center",
                "valign": "vcenter",
                "font_size": 9,
                # "pattern": 1,
                # "bg_color": "gray",
                "fg_color": "#d9d9d9",
            }
        )
        # detail_cell_hedaer_format.set_pattern(1)
        # worksheet_detail.set_row(1, 15, detail_cell_hedaer_format)
        # Write the column headers with the defined format.
        # header 부분은 set_row 함수로 전체 셀이 적용되지 않아 아래 방법으로 처리해야 함
        for col_num, value in enumerate(df_detail.columns.values):
            worksheet_detail.write(0, col_num, value, detail_cell_hedaer_format)

        # worksheet_detail.set_font
        worksheet_detail.set_row(0, 25)
        worksheet_detail.set_column("C:C", 15)
        worksheet_detail.set_column("D:D", 20)

        percent_format = workbook.add_format({"num_format": "0%"})
        worksheet_detail.set_column("O:O", None, percent_format)

        # Close the Pandas Excel writer and output the Excel file.
        # worksheet_detail.write(0, 0, "TEST")

        writer.save()

        return True

    except Exception as err:
        print("[Error] create_excel_file : ", err)
        return err


# %%
def create_excel_file_to_s3(
    data_frame,
    bucket,
    filePath,
    p_sheet_name="sheet",
    calc_cp_info=None,
    calc_month=None,
):
    try:
        # df = pd.DataFrame(
        #     data_frame.get("list"), columns=data_frame.get("headers")
        # )

        df_detail = pd.DataFrame(
            data_frame.get("list"), columns=data_frame.get("headers")
        )

        # to s3
        with io.BytesIO() as output:
            # with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

                # df.to_excel(writer, sheet_name=p_sheet_name, index=False)
                # df_summary = pd.DataFrame([], columns=[])
                # df_summary.to_excel(writer, sheet_name="정산서")

                # ** generate 정산서 요약 시트 데이터 생성 처리 **
                dic_summary = data_handler.generate_calc_summary_data(
                    calc_cp_info, data_frame.get("list"), calc_month
                )

                # summary - 0. title
                df_summary_title = dic_summary["df_title"]

                # summary - 1. total
                df_summary_total_title = dic_summary["total_title"]
                df_summary_total = dic_summary["total"]

                # summary - 2. detail
                df_summary_detail_title = dic_summary["detail_title"]
                df_summary_detail = dic_summary["detail"]

                ###################################
                #   1. sheet - summary
                ###################################
                sheet_summary = dic_summary["sheet_summary"]

                # 0. 제목
                df_summary_title.to_excel(
                    writer,
                    sheet_name=sheet_summary,
                    index=False,
                    startcol=1,
                    startrow=0,
                )
                str_summary_title = list(df_summary_title.columns.values)[0]

                workbook = writer.book
                worksheet_summary = writer.sheets[sheet_summary]

                # print(f"\nworkbook:{workbook}\nworksheet:{worksheet_summary}")

                # cell format
                # worksheet_summary.set_column("B:G", 12)
                currency_format = workbook.add_format({"num_format": "#,##0"})
                # worksheet_summary.set_column("C:G", currency_format)
                worksheet_summary.hide_gridlines(2)
                worksheet_summary.set_column("A:A", 3)
                worksheet_summary.set_column("B:B", 35)
                worksheet_summary.set_column("C:G", 15, currency_format)
                worksheet_summary.set_row(0, 30)

                cell_title_format = workbook.add_format(
                    {
                        "bold": 1,
                        "border": 0,
                        "align": "center",
                        "valign": "vcenter",
                        "font_size": 14,
                        # "fg_color": "yellow",
                    }
                )
                worksheet_summary.merge_range(
                    "B1:G1", str_summary_title, cell_title_format
                )

                # 1.정산 안내
                worksheet_summary.write(3, 1, df_summary_total_title)
                df_summary_total.to_excel(
                    writer,
                    sheet_name=sheet_summary,
                    index=False,
                    startcol=1,
                    startrow=4,
                )

                # 2.정산내역
                worksheet_summary.write(11, 1, df_summary_detail_title)
                df_summary_detail.to_excel(
                    writer,
                    sheet_name=sheet_summary,
                    index=False,
                    startcol=1,
                    startrow=12,
                )

                # 맨하단 안내 문구
                cell_summary_common_guide_format = workbook.add_format({"font_size": 9})
                summary_common_guide_tax = dic_summary["common_guide"]

                # 시작 행 > 12 + detail row
                row_common_start = 12 + len(df_summary_detail) + 3
                for item in summary_common_guide_tax:
                    worksheet_summary.write(
                        row_common_start, 1, item, cell_summary_common_guide_format
                    )
                    row_common_start += 1

                ###################################
                #   2. sheet - detail
                ###################################
                # sheet = 상세내역
                # df_detail = df_detail.style.set_table_attributes(
                #     "style='display:inline'"
                # ).set_caption("Caption table")

                cp_calc_lang = calc_cp_info[9]  # 언어
                locale_contants = data_handler.get_locale_contants(cp_calc_lang)

                sheet_detail = dic_summary["sheet_detail"]

                # 최종 excel 생성 전, 불필요한 컬럼들 삭제 처리
                # 저작권자,저작권자 코드, 매출기준, 정산구분

                detail_headers = locale_contants["detail_columns"]
                detail_cp = detail_headers[1]
                detail_cp_code = detail_headers[19]
                detail_standard_sale = detail_headers[20]
                detail_standard_calc = detail_headers[21]
                df_detail = df_detail.drop(
                    columns=[
                        detail_cp,
                        detail_cp_code,
                        detail_standard_sale,
                        detail_standard_calc,
                    ]
                )

                # sheet 생성
                df_detail.to_excel(
                    writer, sheet_name=sheet_detail, index=False, startcol=0, startrow=0
                )

                worksheet_detail = writer.sheets[sheet_detail]
                detail_cell_hedaer_format = workbook.add_format(
                    {
                        "bold": 1,
                        "border": 1,
                        "align": "center",
                        "valign": "vcenter",
                        "font_size": 9,
                        # "pattern": 1,
                        # "bg_color": "gray",
                        "fg_color": "#d9d9d9",
                    }
                )
                # detail_cell_hedaer_format.set_pattern(1)
                # worksheet_detail.set_row(1, 15, detail_cell_hedaer_format)
                # Write the column headers with the defined format.
                # header 부분은 set_row 함수로 전체 셀이 적용되지 않아 아래 방법으로 처리해야 함
                for col_num, value in enumerate(df_detail.columns.values):
                    worksheet_detail.write(0, col_num, value, detail_cell_hedaer_format)

                # worksheet_detail.set_font
                worksheet_detail.set_row(0, 25)
                worksheet_detail.set_column("C:C", 15)
                worksheet_detail.set_column("D:D", 20)

                percent_format = workbook.add_format({"num_format": "0%"})
                worksheet_detail.set_column("O:O", None, percent_format)

            data = output.getvalue()
        print(f"s3 path : {filePath}")
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).put_object(Key=filePath, Body=data)

        return True

    except Exception as err:
        print("[Error] create_excel_file_to_s3 : ", err)
        return err


# %%
# process - loop data, per cp and create excel file
def process_generate_cp_file(list_sales, dest_path, calc_month, list_calc_cp):
    """process - loop data, per cp and create excel file"""
    try:
        final_list = []
        cp_data_list = []
        now_cp = ""
        report_index = 1
        except_report_list = []
        now_cp_calc_info = None
        now_cp_calc_std = ""

        for row in list_sales:
            # print(f"\nsale item: {row}")
            if now_cp != row["calc_cp_code"]:
                report_index = 1
                now_cp = row["calc_cp_code"]
                # add
                now_cp_calc_info = data_handler.get_cp_info_by_id(now_cp, list_calc_cp)
                if now_cp_calc_info is not None:
                    now_cp_calc_std = now_cp_calc_info[8]
                else:
                    print(f"now_cp_calc_info None:{now_cp_calc_info}")

                if len(cp_data_list) != 0:
                    final_list.append(cp_data_list)

                cp_data_list = []

            if "A선수수익 정산" != row["calc_cp_type_calc"]:

                row_list = None
                if now_cp_calc_std == "총매출":
                    row_list = [
                        report_index,
                        row["calc_cp_name"],
                        row["sales_month"],
                        row["platform"],
                        row["content_title"],
                        row["author"],
                        row["publisher"],
                        row["count_buy"],
                        row["amount_buy"],
                        row["count_rent"],
                        row["amount_rent"],
                        row["count_cancel"],
                        row["amount_cancel"],
                        row["payment_fee_correction"],
                        row["amount_sales"],  # 총매출 -14
                        row["calc_cp_rate_calc"],
                        row["calc_cp_amount_calc"],
                        row["content_series"],
                        row["content_series_code"],
                        row["calc_cp_code"],
                        row["calc_cp_std"],
                        row["calc_cp_type_calc"],
                    ]
                else:  # 순매출
                    row_list = [
                        report_index,
                        row["calc_cp_name"],
                        row["sales_month"],
                        row["platform"],
                        row["content_title"],
                        row["author"],
                        row["publisher"],
                        row["count_buy"],
                        row["amount_buy"],
                        row["count_rent"],
                        row["amount_rent"],
                        row["count_cancel"],
                        row["amount_cancel"],
                        row["payment_fee_correction"],
                        row["amount_sales_forien_fee_correction"],  # 순매출 - 15
                        row["calc_cp_rate_calc"],
                        row["calc_cp_amount_calc"],
                        row["content_series"],
                        row["content_series_code"],
                        row["calc_cp_code"],
                        row["calc_cp_std"],
                        row["calc_cp_type_calc"],
                    ]

                cp_data_list.append(row_list)
                report_index += 1

            # else:  # A선수수익 차감
            #     print(f"\n\n## A선수수익 차감 제외 ## >> {row}")

        print("final list count:{0}".format(len(final_list)))

        # create per cp - excel
        test_index = 0
        except_report_index = 0
        for cp in final_list:
            # locale 정보에 따라 컬럼명 리스트 가져오기
            cp_code = cp[0][19]
            # print("** cp code :", cp_code)
            cp_calc_info = data_handler.get_cp_info_by_id(cp_code, list_calc_cp)
            # print("current cp info ", cp_calc_info)

            if cp_calc_info is None:
                # print("[Exception] not exist cp :", cp)
                print(f"[Exception] not exist cp : {cp_code}")

            else:
                # add - check 정산서 특이 사항 케이스 제외
                flag_except_report = cp_calc_info[14]
                if flag_except_report != 1:
                    cp_locale = cp_calc_info[9]
                    now_cp_detail_columns_name = data_handler.get_detail_columns_name(
                        cp_locale
                    )
                    data_frame = {"list": cp, "headers": now_cp_detail_columns_name}

                    if not os.path.isdir(dest_path):
                        os.mkdir(dest_path)

                    file_name = cp[0][1] + "_" + calc_month + "_정산"
                    # excel_file_name = dest_path + "/" + cp[0][1] + ".xlsx"
                    excel_file_name = dest_path + "/" + file_name + ".xlsx"

                    # execute - create excel file
                    sheet_name = "정산 내역"
                    create_excel_file(
                        data_frame,
                        excel_file_name,
                        sheet_name,
                        cp_calc_info,
                        calc_month,
                    )

                    # # for test
                    # if test_index == 10:
                    #     break

                    # test_index += 1

                else:
                    except_report_index += 1
                    except_report_list.append(cp_calc_info)
                    print(
                        f"\n[Except Report] - ({except_report_index}), {cp_calc_info[3]}({cp_calc_info[1]}), >> flag_except_report='{flag_except_report}' "
                    )

        # finish
        # 처리하지 않은 정산서 목록
        print(f"\n\n*** 예외 처리 필요한 CP 목록 ***\n{except_report_list}")
    except Exception as err:
        print("[Error] process_generate_cp_file : ", err)
        return err


# %%
# cp 정보(tax 포함 유무) 유형에 따라 s3 full path 생성
def generate_s3_file_path(calc_month, cp_name, cp_biz_type, midPath):

    month_folder = calc_month + "-"
    # if include_tax == "면세":
    #     month_folder += "PER"
    # else:
    #     month_folder += "COM"
    if cp_biz_type == "사업자":
        month_folder += "COM"
    else:
        month_folder += "PER"

    file_name = cp_name + "_" + calc_month + "_정산" + ".xlsx"
    # excel_file_name = midPath + "/" + file_name + ".xlsx"
    excel_file_name = midPath + "/" + month_folder + "/" + file_name

    return excel_file_name


# %%
def process_generate_cp_file_to_s3(
    list_sales, calc_month, bucket, midPath, list_calc_cp
):
    try:
        final_list = []
        cp_data_list = []
        now_cp = ""
        report_index = 1
        except_report_list = []
        now_cp_calc_info = None
        now_cp_calc_std = ""

        for row in list_sales:

            if now_cp != row["calc_cp_code"]:
                report_index = 1
                now_cp = row["calc_cp_code"]
                # add
                now_cp_calc_info = data_handler.get_cp_info_by_id(now_cp, list_calc_cp)
                if now_cp_calc_info is not None:
                    now_cp_calc_std = now_cp_calc_info[8]
                else:
                    print(f"now_cp_calc_info None:{now_cp_calc_info}")

                if len(cp_data_list) != 0:
                    final_list.append(cp_data_list)
                cp_data_list = []

            if "A선수수익 정산" != row["calc_cp_type_calc"]:

                row_list = None
                if now_cp_calc_std == "총매출":
                    row_list = [
                        report_index,
                        row["calc_cp_name"],
                        row["sales_month"],
                        row["platform"],
                        row["content_title"],
                        row["author"],
                        row["publisher"],
                        row["count_buy"],
                        row["amount_buy"],
                        row["count_rent"],
                        row["amount_rent"],
                        row["count_cancel"],
                        row["amount_cancel"],
                        row["payment_fee_correction"],
                        row["amount_sales"],  # 총매출 -14
                        row["calc_cp_rate_calc"],
                        row["calc_cp_amount_calc"],
                        row["content_series"],
                        row["content_series_code"],
                        row["calc_cp_code"],
                        row["calc_cp_std"],
                        row["calc_cp_type_calc"],
                    ]
                else:  # 순매출
                    row_list = [
                        report_index,
                        row["calc_cp_name"],
                        row["sales_month"],
                        row["platform"],
                        row["content_title"],
                        row["author"],
                        row["publisher"],
                        row["count_buy"],
                        row["amount_buy"],
                        row["count_rent"],
                        row["amount_rent"],
                        row["count_cancel"],
                        row["amount_cancel"],
                        row["payment_fee_correction"],
                        row["amount_sales_forien_fee_correction"],  # 순매출 - 15
                        row["calc_cp_rate_calc"],
                        row["calc_cp_amount_calc"],
                        row["content_series"],
                        row["content_series_code"],
                        row["calc_cp_code"],
                        row["calc_cp_std"],
                        row["calc_cp_type_calc"],
                    ]

                cp_data_list.append(row_list)
                report_index += 1

            else:  # A선수수익 차감
                print(f"\n\n## A선수수익 차감 제외 ## >> {row}")

        print("final list count:{0}".format(len(final_list)))

        # create per cp - excel
        test_index = 0
        except_report_index = 0

        for cp in final_list:
            # locale 정보에 따라 컬럼명 리스트 가져오기
            cp_code = cp[0][19]
            # print("** cp code :", cp_code)
            cp_calc_info = data_handler.get_cp_info_by_id(cp_code, list_calc_cp)

            if cp_calc_info is None:
                print("[Exception] not exist cp @s3 :", cp)

            else:
                # add - check 정산서 특이 사항 케이스 제외
                flag_except_report = cp_calc_info[14]
                if flag_except_report != 1:
                    cp_locale = cp_calc_info[9]
                    now_cp_detail_columns_name = data_handler.get_detail_columns_name(
                        cp_locale
                    )
                    data_frame = {"list": cp, "headers": now_cp_detail_columns_name}
                    # data_frame = {"list": cp, "headers": list_detail_headers}

                    cp_name = cp[0][1]
                    # flag_include_tax = cp[0][19]
                    cp_biz_type = cp_calc_info[5]  # 사업자 유형

                    excel_file_name = generate_s3_file_path(
                        calc_month, cp_name, cp_biz_type, midPath
                    )

                    # execute - create excel file
                    sheet_name = "정산 내역"
                    create_excel_file_to_s3(
                        data_frame,
                        bucket,
                        excel_file_name,
                        sheet_name,
                        cp_calc_info,
                        calc_month,
                    )

                    # index += 1
                    print("create excel file to s3 ", bucket, excel_file_name)
                    # break

                    # for test
                    if test_index == 10:
                        break

                    test_index += 1

                else:
                    except_report_index += 1
                    except_report_list.append(cp_calc_info)
                    print(
                        f"\n[Except Report] - ({except_report_index}), {cp_calc_info[3]}({cp_calc_info[1]}) "
                    )

        # finish
        # 처리하지 않은 정산서 목록
        print(f"\n\n*** 예외 처리 필요한 CP 목록 ***\n{except_report_list}")

    except Exception as err:
        print("[Error] process_generate_cp_file : ", err)
        return err


def generate_all_cp_reports(event):
    # 0. get data
    # result_set = get_data_from_db("public.RawCalcList")
    calc_month = event["calcMonth"]  # "2022-01"
    # result_set = get_calc_db_data('public."RawCalcList"', calc_month)

    # 해당 월 정산 데이터
    list_calc_data = data_handler.get_calc_data_from_db(
        event["sourceTable"], calc_month
    )
    # 정산서 생성에 필요한 cp 정보들
    list_calc_cp = data_handler.get_calc_cp_all_info(from_source="local")

    # print("calc_cp:\n", result_set_calc_cp)

    # test
    # ret_cp = data_handler.get_cp_info_by_id("CPDV00010_01", list_calc_cp)
    # print("find cp:\n", ret_cp)

    # print("result = {0}".format(len(list_calc_data)))
    if list_calc_data != None:
        print("success - get db data")
        # for row in result_set:
        #     print(row['copyrightholder'])
        output_path = "./output"
        bucket = event["targetS3Bucket"]
        midPath = event["targetS3midPath"]

        # add 22.06.
        outputEnv = event["outputEnv"]
        print(f"outputEnv >> {outputEnv}")
        if outputEnv == "local":
            # @local
            print("\nprocess to local\n\n")
            process_generate_cp_file(
                list_calc_data, output_path, calc_month, list_calc_cp
            )
        else:
            print("process to S3")
            process_generate_cp_file_to_s3(
                list_calc_data, calc_month, bucket, midPath, list_calc_cp
            )

        # get folder size
        # folder_size_bytes = utils.get_size(output_path)
        # print("## output folder size : {:.1f}".format(folder_size_bytes / 1024))
    else:
        print("[info] get read data is none")


# %%
def lambda_handler(event, context):

    generate_all_cp_reports(event)

    print("\n\nEnd")


# %%
if __name__ == "__main__":
    test_event_local = {
        "sourceTable": "RawCalcList",
        "calcMonth": "2022-06",
        "targetS3Bucket": "myattatchbuket",
        "targetS3midPath": "calc_cp_working",
        "outputEnv": "local",
    }
    test_event_aws = {
        "sourceTable": "RawCalcList",
        "calcMonth": "2022-03",
        "targetS3Bucket": "myattatchbuket",
        "targetS3midPath": "calc_cp_working",
        "outputEnv": "s3",
    }
    test_event = test_event_local

    lambda_handler(test_event, None)  # test for local-excel
