import os
import sys
import json
import pandas as pd

# from io import BytesIO
import io
import boto3

# import xlsxwriter
# import data_handler

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


def func_name():
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name


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
        # set add primary key
        # alter table public."calc_raw_2021-01" add column "id" serial not null primary key;
        sql = (
            "SELECT * FROM public.\"{0}\" Where calc_month='{1}' "
            "ORDER BY calc_cp_name, content_title, platform;".format(
                table_name, calc_month
            )
        )
        result = db_engine.execute(sql)
        result_set = result.fetchall()

        return result_set

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


def create_excel_file(data_frame, dest_excel_file, p_sheet_name="sheet"):
    try:
        # print("# create_excel_file > data_frame headers : {0}".format(
        #     data_frame.get('headers')))
        # print("# create_excel_file > data_frame list : {0}".format(
        #     data_frame.get('list')))
        df = pd.DataFrame(data_frame.get("list"), columns=data_frame.get("headers"))
        # print(df)
        # create excel file
        print("** create excel file : " + dest_excel_file)
        # df.to_excel(dest_excel_file, sheet_name=p_sheet_name)

        # Create a Pandas Excel writer using XlsxWriter as the engine.
        df_summary = pd.DataFrame([], columns=[])
        writer = pd.ExcelWriter(dest_excel_file)
        # Write each dataframe to a different worksheet.
        df_summary.to_excel(writer, sheet_name="정산서")
        df.to_excel(writer, sheet_name=p_sheet_name, index=False)
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

        return True

    except Exception as err:
        print("[Error] create_excel_file : ", err)
        return err


# %%
def create_excel_file_to_s3(data_frame, bucket, filePath, p_sheet_name="sheet"):
    try:
        df = pd.DataFrame(data_frame.get("list"), columns=data_frame.get("headers"))

        # # Create a Pandas Excel writer using XlsxWriter as the engine.
        # df_summary = pd.DataFrame([], columns=[])
        # writer = pd.ExcelWriter(dest_excel_file)

        # # Write each dataframe to a different worksheet.
        # df_summary.to_excel(writer, sheet_name="정산서")
        # df.to_excel(writer, sheet_name=p_sheet_name, index=False)
        # # Close the Pandas Excel writer and output the Excel file.
        # writer.save()

        # to s3
        with io.BytesIO() as output:
            # with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with pd.ExcelWriter(output) as writer:
                # df.to_excel(writer, 'sheet_name')
                df.to_excel(writer, sheet_name=p_sheet_name, index=False)
                df_summary = pd.DataFrame([], columns=[])
                df_summary.to_excel(writer, sheet_name="정산서")
            data = output.getvalue()
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).put_object(Key=filePath, Body=data)

        return True

    except Exception as err:
        print("[Error] create_excel_file_to_s3 : ", err)
        return err


# %%
# process - loop data, per cp and create excel file


def process_generate_cp_file(data, dest_path, calc_month):
    try:
        final_list = []
        cp_data_list = []
        now_cp = ""
        report_index = 1
        for row in data:

            if now_cp != row["calc_cp_name"]:
                report_index = 1
                now_cp = row["calc_cp_name"]
                if len(cp_data_list) != 0:
                    final_list.append(cp_data_list)
                cp_data_list = []

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
                row["amount_sales"],
                row["calc_cp_rate_calc"],
                row["calc_cp_amount_calc"],
                row["content_series"],
                row["content_series_code"],
            ]

            cp_data_list.append(row_list)
            report_index += 1
        print("final list count:{0}".format(len(final_list)))

        # create per cp - excel
        list_headers = [
            "순번",
            "저작권자",
            "판매월",
            "서비스",
            "제목",
            "저자",
            "출판사",
            "구매 건수",
            "구매 금액",
            "대여건수",
            "대여금액",
            "취소건수",
            "취소금액",
            "결제수수료 보정액",
            "매출",
            "저작권자 정산율",
            "저작권자 정산금",
            "시리즈명",
            "시리즈번호",
        ]
        for cp in final_list:
            data_frame = {"list": cp, "headers": list_headers}

            if not os.path.isdir(dest_path):
                os.mkdir(dest_path)

            file_name = cp[0][1] + "_" + calc_month + "_정산"
            # excel_file_name = dest_path + "/" + cp[0][1] + ".xlsx"
            excel_file_name = dest_path + "/" + file_name + ".xlsx"

            # execute - create excel file
            sheet_name = "정산 내역"
            create_excel_file(data_frame, excel_file_name, sheet_name)

    except Exception as err:
        print("[Error] process_generate_cp_file : ", err)
        return err


# %%
# cp 정보(tax 포함 유무) 유형에 따라 s3 full path 생성
def generate_s3_file_path(calc_month, cp_name, include_tax, midPath):

    month_folder = calc_month + "-"
    if include_tax == "면세":
        month_folder += "PER"
    else:
        month_folder += "COM"

    file_name = cp_name + "_" + calc_month + "_정산" + ".xlsx"
    # excel_file_name = midPath + "/" + file_name + ".xlsx"
    excel_file_name = midPath + "/" + month_folder + "/" + file_name

    return excel_file_name


# %%
def process_generate_cp_file_to_s3(data, calc_month, bucket, midPath):
    try:
        final_list = []
        cp_data_list = []
        now_cp = ""
        report_index = 1
        for row in data:

            if now_cp != row["calc_cp_name"]:
                report_index = 1
                now_cp = row["calc_cp_name"]
                if len(cp_data_list) != 0:
                    final_list.append(cp_data_list)
                cp_data_list = []

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
                row["amount_sales"],
                row["calc_cp_rate_calc"],
                row["calc_cp_amount_calc"],
                row["content_series"],
                row["content_series_code"],
                row["flag_include_tax"],
            ]

            cp_data_list.append(row_list)
            report_index += 1
        print("final list count:{0}".format(len(final_list)))

        # create per cp - excel
        list_headers = [
            "순번",
            "저작권자",
            "판매월",
            "서비스",
            "제목",
            "저자",
            "출판사",
            "구매 건수",
            "구매 금액",
            "대여건수",
            "대여금액",
            "취소건수",
            "취소금액",
            "결제수수료 보정액",
            "매출",
            "저작권자 정산율",
            "저작권자 정산금",
            "시리즈명",
            "시리즈번호",
            "부가세",
        ]
        for cp in final_list:
            data_frame = {"list": cp, "headers": list_headers}

            # if not os.path.isdir(dest_path):
            #     os.mkdir(dest_path)

            # file_name = cp[0][1] + "_" + calc_month + "_정산"
            # # # excel_file_name = dest_path + "/" + file_name + ".xlsx"
            # excel_file_name = midPath + "/" + file_name + ".xlsx"
            cp_name = cp[0][1]
            flag_include_tax = cp[0][19]

            excel_file_name = generate_s3_file_path(
                calc_month, cp_name, flag_include_tax, midPath
            )

            # execute - create excel file
            sheet_name = "정산 내역"
            create_excel_file_to_s3(data_frame, bucket, excel_file_name, sheet_name)
            print("create excel file to s3 ", bucket, excel_file_name)
            break

    except Exception as err:
        print("[Error] process_generate_cp_file : ", err)
        return err


# %%
def lambda_handler(event, context):
    # 0. get data
    # result_set = get_data_from_db("public.RawCalcList")
    calc_month = event["calcMonth"]  # "2022-01"
    # result_set = get_calc_db_data('public."RawCalcList"', calc_month)
    result_set = get_calc_data_from_db(event["sourceTable"], calc_month)
    print("result = {0}".format(len(result_set)))
    if result_set != None:
        print("success - get db data")
        # for row in result_set:
        #     print(row['copyrightholder'])
        output_path = "./output"
        bucket = event["targetS3Bucket"]
        midPath = event["targetS3midPath"]
        process_generate_cp_file_to_s3(result_set, calc_month, bucket, midPath)

        # get folder size
        # folder_size_bytes = utils.get_size(output_path)
        # print("## output folder size : {:.1f}".format(folder_size_bytes / 1024))
    else:
        print("[info] get read data is none")

    print("\n\nEnd")


# %%
if __name__ == "__main__":
    test_event_local = {}
    test_event_aws = {
        "sourceTable": "RawCalcList",
        "calcMonth": "2022-01",
        "targetS3Bucket": "myattatchbuket",
        "targetS3midPath": "calc_cp_working",
    }
    test_event = test_event_aws

    lambda_handler(test_event, None)  # test for local-excel
