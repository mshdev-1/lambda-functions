import os
import sys
import json
import pandas as pd
from io import BytesIO
import data_handler

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


def func_name():
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name


def read_s3_file(bucket_name, key):
    try:
        import layers.s3Manager.python.s3Manager as S3Manager

        # get s3 file download
        print("# get s3 #0")
        s3mgr = S3Manager.S3Manager(region_name="ap-northeast-2")
        file_obj = s3mgr.get_object(bucket_name, key)
        print("sucess get s3 file")
        return file_obj

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return False


# read data from excel file object
def read_data_from_excel_obj(file_obj, i_sheet_name, i_header=0):
    try:
        # io.BytesIO(obj['Body'].read()
        # df = pd.read_excel(file_obj["Body"], sheet_name=i_sheet_name, header=i_header)
        df = pd.read_excel(
            BytesIO(file_obj["Body"].read()), sheet_name=i_sheet_name, header=i_header
        )
        return df

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


# read data from excel file object
def read_data_from_csv_obj(file_obj, i_header=0):
    try:
        print("# START - read_data_from_csv_obj")
        # io.BytesIO(obj['Body'].read()
        # df = pd.read_excel(file_obj["Body"], sheet_name=i_sheet_name, header=i_header)
        df = pd.read_csv(BytesIO(file_obj["Body"].read()), header=i_header)
        return df

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


# read data from excel file
def read_data_from_excel_file(read_file_name, i_sheet_name, i_header=0):
    try:
        df = pd.read_excel(
            open(read_file_name, "rb"), sheet_name=i_sheet_name, header=i_header
        )

        #  df = pd.read_excel(open(read_file_name, 'rb'),
        #                    sheet_name=i_sheet_name, header=i_header)
        return df

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


# read data from s3 excel file
def read_data_from_s3_file_excel(bucket, key, i_sheet_name, i_header=0):
    try:
        df = pd.read_excel(
            f"s3://{bucket}/{key}", sheet_name=i_sheet_name, header=i_header
        )
        return df

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))
        return None


def save_db_table(df, table_name, from_source="s3"):
    try:
        if from_source == "s3":
            import layers.dbManager.python.dbManager as DBManager
        else:
            sys.path.append("../layers")
            from layers.dbManager.python import dbManager as DBManager

        dbMgr = DBManager.DBManager()
        db_engine = dbMgr.get_engine()
        df.to_sql(name=table_name, con=db_engine, if_exists="replace", index=True)
        # set add primary key
        # alter table public."calc_raw_2021-01" add column "id" serial not null primary key;
        sql = (
            'alter table public."{0}" add column '
            "id serial not null primary key;".format(table_name)
        )
        db_engine.execute(sql)

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))


def optimization_table_columns(table_name):
    try:
        import layers.dbManager.python.dbManager as DBManager

        dbMgr = DBManager.DBManager()
        db_engine = dbMgr.get_engine()

        sql = 'alter table public."{0}" alter column calc_amount_sales  type int using cast_to_numeric(calc_amount_sales), alter column calc_amount_net_sales  type int using cast_to_numeric(calc_amount_net_sales), alter column calc_content_id  type int using cast_to_numeric(calc_content_id), alter column calc_cp_id  type int using cast_to_numeric(calc_cp_id);'.format(
            table_name
        )
        db_engine.execute(sql)

        sql = 'alter table public."{0}" alter column calc_rate_cp  type int, alter column calc_amount_cp  type int, alter column calc_flag_mg type char(1) ;'.format(
            table_name
        )
        db_engine.execute(sql)

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))


def generate_calc_raw_table_name(event_params):
    try:

        return event_params["tableName"]

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))


def generate_calc_raw_file_s3_path(calc_month):
    s3_path = calc_month + "-Calc-Data/" + calc_month + "-Calc-Raw.xlsx"

    return s3_path


def generate_calc_raw_table_name_old(s3_keyname):
    try:
        # *** calculate_data/sample-min-cms-calc-raw_2021-01.csv ****
        # path split "/"
        list_path = s3_keyname.split("/")
        file_name = list_path[len(list_path) - 1]

        # 0. split "_"
        list_file = file_name.split("_")

        # 1. split "." from .csv file name
        list_file_name = list_file[1].split(".")

        # combine string
        table_name = "RawCalc_" + list_file_name[0]
        return table_name

    except Exception as e:
        print("[Error] {0} : {1} ".format(func_name(), e))


def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))
        # result - default
        statusCode = 299
        success = False

        result_body = {"success": success}

        final_result = {
            "statusCode": statusCode,
            "success": success,
            "body": json.dumps(result_body),
        }

        # check - essential params
        calc_month = None
        if "calcMonth" in event:
            calc_month = event["calcMonth"]
        else:
            print("not exist essential params")

        df = None
        # source = event["source"]
        # fixed - server side items
        source = "s3"
        bucket_name = "myattatchbuket"
        start_row = 2  # event["startRow"]
        sheet_name = "정산내역"  # event["sheetName"]
        # key
        key = generate_calc_raw_file_s3_path(calc_month)
        # dest table name
        dest_table_name = "RawCalcList"

        if source == "s3":
            # bucket_name = event["bucket"]
            # key = event["key"]

            print("#1 - START - get s3")
            # download_file = "calc_raw.xlsx"
            file_obj = read_s3_file(bucket_name, key)

            if not file_obj:
                print("\t#1-1 s3 file object None, Finish")
                return
            print("#1 - END - get s3")
            # get read download file
            # print("#2 START - read_data_from_csv_obj")
            # df = read_data_from_csv_obj(file_obj, 0)
            # print("#2 END - read_data_from_csv_obj")
            print("#2 START - read_data_from_csv_obj")
            df = read_data_from_excel_obj(file_obj, sheet_name, start_row)
            print("#2 END - read_data_from_csv_obj")

        elif source == "local":
            print("#1 - START - get local file")
            local_file = event["fileName"]
            sheet_name = event["sheetName"]

            excel_file_path = os.getcwd() + local_file

            # if not file_obj:
            #     print("\t#1-1 s3 file object None, Finish")
            #     return
            # print("#1 - END - get s3")

            # get file type extension, csv or excel
            list_file_name = local_file.split(".")
            ext_file = list_file_name[len(list_file_name) - 1]
            print("# > read file exe - {0}".format(ext_file))
            # get read file
            print("#2 START - read_data_from_excel_file")
            if ext_file == "csv":
                df = read_data_from_csv_obj(excel_file_path, 2)
            else:
                start_row = event["startRow"]
                df = read_data_from_excel_file(excel_file_path, sheet_name, start_row)
            print("#2 END - read_data_from_excel_file")

        #### Data Converting #####
        # 21.11.30 - 현 단계에서는 데이타 가공이 필요없어 패스
        # convert_df = df
        # convert_df = data_handler.calc_proc_data_convert(df)
        # 22.02
        convert_df = data_handler.calc_proc_data_convert(df)

        if convert_df is None:
            print("\n\n*** exit - error processing converting data ***\n\n")

            result_body = {"error": "error processing converting data "}
            final_result["body"] = json.dumps(result_body)
            return final_result
            exit()

        # return final_result

        # save db
        print("#3 START - save to db")
        # key = local_file
        # modify - fix table name
        # table_name = generate_calc_raw_table_name(event)
        save_db_table(convert_df, dest_table_name, source)
        print(">> save table = " + dest_table_name)
        print("#3 END - save to db")

        # save db
        print("#4 START - db optimization columns type")
        # 아래 colums 최적화 부분은 다시 확인 필요. 21.06.10
        # optimization_table_columns(table_name)
        print("#4 END - db optimization columns type")

        print("### end process ###")
    except Exception as e:
        print("[Error] lambda_handler : ", e)


if __name__ == "__main__":
    _event_excel_s3 = {
        # "source": "s3",
        "calcMonth": "2023-01"
        # "bucket": "msh-cms-upload",
        # "key": "calculate_data/sample-cms-calc-raw_2021-03.xlsx",
    }

    _event_csv = {
        "source": "s3",
        "bucket": "msh-cms-upload",
        "key": "calculate_data/sample-cms-calc-raw_2021-01.csv",
    }

    _event_csv_min = {
        "source": "s3",
        "bucket": "msh-cms-upload",
        "key": "calculate_data/sample-min-cms-calc-raw_202101.csv",
    }

    # test for by local file
    _event_local_min_excel = {
        "source": "local",
        "fileName": "/data/RawCalc_2022-01.min.xlsx",
        "tableName": "RawCalcList",
        "sheetName": "정산내역",
        "startRow": 1,
    }

    # test for by local file
    _event_local_full_excel = {
        "source": "local",
        "fileName": "/data/RawCalc_2022-01.xlsx",
        "tableName": "RawCalcList",
        "sheetName": "정산내역",
        "startRow": 2,
    }
    # 2021년 6월 정산 데이터통합_2021-06-full
    _event_local_full_csv = {
        "source": "local",
        "fileName": "/data/RawCalc_2021-11.csv",
        "sheetName": "정산내역",
        "startRow": 2,
    }

    # lambda_handler(_event_csv_min, None)  #test for s3-csv

    lambda_handler(_event_excel_s3, None)  # test for local-excel
