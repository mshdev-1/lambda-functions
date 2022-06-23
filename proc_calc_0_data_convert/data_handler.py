from cmath import isnan
import os
import sys
import pandas as pd
import traceback


def func_name():
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name


def strip_non_ascii(string):
    """ Returns the string without non ASCII characters"""
    stripped = (c for c in string if 0 < ord(c) < 127)
    return "".join(stripped)


def remove_all_unicode(string):
    return "".join([i if ord(i) < 128 else " " for i in string])


def calc_proc_data_convert_old(dataframe):
    """
    :input: pandas dataframe
    :return:
    """
    try:
        print("processing...")

        # 1. add columns - 계산용 총매출, 순매출 (기존 컬럼 'amount_sales', 'amount_net_sales')
        # print("head(10)", dataframe.columns(0))

        # :: .csv 파일 읽어왔을때, 특정 columns name에 특수문자가 포함되어 있어, 전체 columns rename 처리
        # format - 21.04 version
        # format_columns_old = [
        #     "doc_no",
        #     "sales_month",
        #     "platform",
        #     "content_title",
        #     "author",
        #     "publisher",
        #     "cps",
        #     "count_free",
        #     "count_buy",
        #     "amount_buy",
        #     "count_rent",
        #     "amount_rent",
        #     "count_freepass",
        #     "amount_freepass",
        #     "count_cancel",
        #     "amount_cancel",
        #     "amount_sales",
        #     "rate_platform",
        #     "amount_net_sales",
        #     "rate_cp_org",
        #     "amount_cp_org",
        #     "amount_revenue_org",
        #     "content_type",
        # ]

        # format - 21.06.10
        format_columns = [
            "doc_no",
            "sales_month",
            "platform",
            "content_title",
            "author",
            "publisher",
            "cps",
            "count_free",
            "count_buy",
            "amount_buy",
            "count_rent",
            "amount_rent",
            "count_freepass",
            "amount_freepass",
            "count_cancel",
            "amount_cancel",
            "amount_sales",
            "rate_platform",
            "amount_net_sales",
            "sum_rate_cps_org",
            "sum_amount_cps_org",
            "amount_revenue_org",
            "content_type",
            "flag_issuance_tax_invoice",
            "content_series",
            "content_code",
            "description",
            "sales_type",
            "month_calc",
            "amount_calc_foreign_currency",
            "currency",
            "flag_include_tax",
            "cp1_name",
            "cp1_code",
            "cp1_calc_std",
            "cp1_rate_calc",
            "cp1_amount_calc",
            "cp1_type_calc",
            "cp2_name",
            "cp2_code",
            "cp2_calc_std",
            "cp2_rate_calc",
            "cp2_amount_calc",
            "cp2_type_calc",
            "cp3_name",
            "cp3_code",
            "cp3_calc_std",
            "cp3_rate_calc",
            "cp3_amount_calc",
            "cp3_type_calc",
            "cp4_name",
            "cp4_code",
            "cp4_calc_std",
            "cp4_rate_calc",
            "cp4_amount_calc",
            "cp4_type_calc",
            "platform_code",
            "calc_revenue_std",
            "flag_revenue",
            "content_series_code",
            "platform_details",
            "sales_apply_month",
            "accounting_platform",
            "accounting_date_proof",
            "accounting_date_verify",
        ]

        # format - 22.01
        format_columns = [
            "sales_code",
            "calc_month",
            "sales_month",
            "platform",
            "platform_detail",
            "platform_code",
            "platform_detail_code",
            "platform_locale",
            "platform_content_code",
            "content_title",
            "author",
            "publisher",
            "cps",
            "count_free",
            "count_buy",
            "amount_buy",
            "count_rent",
            "amount_rent",
            "count_freepass",
            "amount_freepass",
            "count_cancel",
            "amount_cancel",
            "payment_fee_correction",
            "amount_sales",
            "rate_platform",
            "amount_net_sales",
            "amount_sales_forien_fee_correction",
            "sum_rate_cps_org",
            "sum_amount_cps_org",
            "amount_revenue_org",
            "content_type",
            # "flag_issuance_tax_invoice",
            "content_code",
            "content_series",
            "content_series_code",
            # "description",
            # "month_calc",
            "sales_type",
            "advance_payment_mgr_code",  # 선수수익 관리코드
            "amount_calc_foreign_currency",
            "currency",
            "exchange_rate",
            "foriegn_in_rate",
            "description",
            "tax_correction",
            "flag_include_tax",
            "cp1_name",
            "cp1_code",
            "cp1_calc_code",
            "cp1_calc_std",
            "cp1_rate_calc",
            "cp1_except_advanced_rate_calc",
            "cp1_amount_calc",
            "cp1_type_calc",
            "cp1_description",
            "cp2_name",
            "cp2_code",
            "cp2_calc_code",
            "cp2_calc_std",
            "cp2_rate_calc",
            "cp2_except_advanced_rate_calc",
            "cp2_amount_calc",
            "cp2_type_calc",
            "cp2_description",
            "cp3_name",
            "cp3_code",
            "cp3_calc_code",
            "cp3_calc_std",
            "cp3_rate_calc",
            "cp3_except_advanced_rate_calc",
            "cp3_amount_calc",
            "cp3_type_calc",
            "cp3_description",
            "cp4_name",
            "cp4_code",
            "cp4_calc_code",
            "cp4_calc_std",
            "cp4_rate_calc",
            "cp4_except_advanced_rate_calc",
            "cp4_amount_calc",
            "cp4_type_calc",
            "cp4_description",
            # 이후 확인 필요
            "sales_apply_month",
            "sales_accounting_1",
            "bill_issuance",
            "sales_accounting_2",
            "accounting_proof_no",
            "accounting_date_verify"
            # ,
            # "platform_code",
            # "calc_revenue_std",
            # "flag_revenue",
            # "content_series_code",
            # "platform_details",
            # "sales_apply_month",
            # "accounting_platform",
            # "accounting_date_proof",
            # "accounting_date_verify",
        ]

        dataframe.columns = format_columns

        # dataframe.columns = [col.encode("ascii", "ignore") for col in dataframe.columns]
        # dataframe.columns = [strip_non_ascii(col) for col in dataframe.columns]
        # dataframe.columns = [str(col) for col in dataframe.columns]
        # for col, dtype in dataframe.dtypes.items():
        #     print("$$ object column dtype >> ", dtype)
        #     if dtype == np.object:  # Only process byte object columns.
        #         print("$$ object column >> ", col)
        #         dataframe[col] = dataframe[col].astype(str)
        #     #     dataframe[col] = dataframe[col].apply(lambda x: x.decode("utf-8"))
        # for col, dtype in dataframe.dtypes.items():
        #     print("$$ ## object column dtype >> ", dtype)

        # dataframe = dataframe.applymap(
        #     lambda x: x.decode() if isinstance(x, bytes) else x
        # )
        # dataframe.columns = dataframe.columns.str.replace(" ", "")

        # @add columns - assign, 복제 컬럼
        new_df = dataframe.assign(
            cp_org=lambda x: x["cp"],
            calc_amount_sales=lambda x: x["amount_sales"],
            calc_amount_net_sales=lambda x: x["amount_net_sales"],
        )

        # calc_content_id="",
        # calc_cp_id="",
        # calc_rate_cp=0,
        # calc_amount_cp=0,
        # calc_flag_mg=0,
        new_df["calc_content_id"] = ""
        new_df["calc_cp_id"] = 0
        new_df["calc_rate_std"] = ""  # 정산 기준 금액 - 총매출 or 순매출
        new_df["calc_rate_std_code"] = ""  # 정산 기준 금액 - 총매출=sales or 순매출=net_sales
        new_df["calc_rate_cp"] = 0
        new_df["calc_amount_cp"] = 0
        new_df["calc_flag_mg"] = 0
        new_df["platform_id"] = 0

        # covert_dict = {'A': int,
        #         'C': float
        #        }

        # create default index
        # new_df = new_df.reset_index(drop=True)
        print("> org df rows = ", len(dataframe))

        duplicated_df = new_df.query('cp.str.contains(",")')
        print("#duplicated_df ", duplicated_df)
        ret_df = new_df
        # df_list = list(duplicated_df)

        for i in range(duplicated_df.shape[0] - 1, -1, -1):
            print("#duplicated_df >> i", i)
            print("#duplicated_df >> row - iloc ", duplicated_df.iloc[i])
            # print("#duplicated_df >> row - loc ", duplicated_df.loc[i])
            # for row in df_list:
            # cps = row["cp"].split(",")
            row = duplicated_df.iloc[i]
            cps = duplicated_df.iloc[i]["cp"].split(",")
            print("#duplicated_df >> cps", duplicated_df.iloc[i]["cp"])
            # @기존 row 삭제
            if len(cps) > 1:
                print(" --- drop row index=", i)
                print(" --- drop row =", row.name)
                new_df.drop(row.name, inplace=True)

            index = 0
            for i in cps:
                # print("** cp name - ", i.strip())
                # print("** index - ", index)

                # @todo index 확인
                row["cp"] = i.strip()
                if index > 0:
                    row["amount_sales"] = 0
                    row["amount_net_sales"] = 0

                new_df = new_df.append(row)
                index += 1

        # for i, row in duplicated_df.iterrows():
        #     print("#duplicated_df >> i", i)
        #     print("#duplicated_df >> index", row[i])
        #     # for row in df_list:
        #     cps = row["cp"].split(",")

        #     # @기존 row 삭제
        #     if len(cps) > 1:
        #         print(" --- drop row index=", i)
        #         print(" --- drop row =", new_df[i])
        #         # new_df = new_df.drop(new_df.index[i])
        #         new_df = ret_df.drop(ret_df[i])
        #         # new_df = ret_df.drop(index=i)

        #     # index = 0
        #     # for i in cps:
        #     #     # print("** cp name - ", i.strip())
        #     #     # print("** index - ", index)

        #     #     # @todo index 확인
        #     #     row["cp"] = i.strip()
        #     #     if index > 0:
        #     #         row["amount_sales"] = 0
        #     #         row["amount_net_sales"] = 0

        #     #     new_df = new_df.append(row)
        #     #     index += 1

        # @replcae data - remove '%'
        # new_df["rate_platform"].replace({"%", ""}, inplace=True)
        # new_df["rate_cp_org"].replace({"%", ""}, inplace=True)

        print("# new_df[rate_platform] ", new_df["rate_platform"])
        print("# new_df[rate_platform].dtypes ", new_df["rate_platform"].dtypes)
        # comment by 21.06.10
        # new_df["rate_platform"] = new_df["rate_platform"].str.replace("%", "")
        # new_df["rate_cp_org"] = new_df["rate_cp_org"].str.replace("%", "")

        # new_df.replace({"%", "***"}, inplace=True)

        # @add rows - multiple cp's
        # list_cp = dataframe["cp"].str.split(",")
        # print("> list_cp = ", list_cp)

        # repeat
        # repeat with assign scalar 10
        # df1 = dataframe.loc[dataframe.index.repeat(2)].assign(cp="**cp**")

        # new_df = dataframe.append[dataframe.index.repeat(len(list_cp))]
        print("> append new_df rows = ", len(new_df))

        # new_df = dataframe.assign(calc_amount_sales=0)
        # new_df.head(10)
        # print("head(10)", new_df.loc[:, "cp":"cp_org"])
        print("head(10)", new_df.loc[:, "amount_sales":"calc_flag_mg"])
        # print(
        #     "head(10)",
        #     new_df.loc["cp_org", "cp"],
        #     # new_df.loc[["cp_org", "cp", " amount_sales", " amount_net_sales"]],
        # )
        # print("head(10)", new_df)
        # @
        # print("head(10)", ret_df.loc[:, ["No", "cp"]])

        return new_df

    except Exception as e:
        print("[Error] @{0} : {1}".format(func_name(), e))


def convert_row_data(row_data, cp_index, row_index):
    cp_head = "cp" + str(cp_index)
    cp_name = row_data[cp_head + "_name"]
    new_row_data = row_data.copy()
    print("# cp_head: ", cp_head)
    # print("# row_index: isinstance", row_index, "cp_name : ", isinstance(cp_name, str))
    if isinstance(cp_name, str) and len(cp_name) > 1:
        # print("\n\n ** row cp name = ", row_data["cp1_name"], " ** \n\n")
        new_row_data["calc_cp_name"] = row_data[cp_head + "_name"]
        # print(" OK - assign new cp name")

        # new_row_data["calc_cp_name"] = row_data[str(cp_head + "_name")]
        new_row_data["calc_cp_code"] = row_data[cp_head + "_code"]
        new_row_data["calc_cp_calc_code"] = row_data[str(cp_head + "_calc_code")]
        new_row_data["calc_cp_std"] = row_data[str(cp_head + "_calc_std")]
        new_row_data["calc_cp_rate_calc"] = row_data[str(cp_head + "_rate_calc")]
        new_row_data["calc_cp_except_advanced_rate_calc"] = row_data[
            str(cp_head + "_except_advanced_rate_calc")
        ]
        new_row_data["calc_cp_amount_calc"] = row_data[str(cp_head + "_amount_calc")]
        new_row_data["calc_cp_type_calc"] = row_data[str(cp_head + "_type_calc")]
        new_row_data["calc_cp_description"] = row_data[str(cp_head + "_description")]

        ### remove cp columns ###
        # cp1_name
        # cp1_code
        # cp1_calc_code
        # cp1_calc_std
        # cp1_rate_calc
        # cp1_except_advanced_rate_calc
        # cp1_amount_calc
        # cp1_type_calc
        # cp1_description
        for del_index in range(4):
            del_cp_head = "cp" + str(del_index + 1)
            new_row_data = new_row_data.drop(
                labels=[
                    del_cp_head + "_name",
                    del_cp_head + "_code",
                    del_cp_head + "_calc_code",
                    del_cp_head + "_calc_std",
                    del_cp_head + "_rate_calc",
                    del_cp_head + "_except_advanced_rate_calc",
                    del_cp_head + "_amount_calc",
                    del_cp_head + "_type_calc",
                    del_cp_head + "_description",
                ]
            )

    else:
        print("\n\n ** row_data None = ", row_index, " ** \n\n")
        # new_row_data = None
        new_row_data = pd.Series([])

    # print("# new_row_data ", new_row_data)
    return new_row_data


def calc_proc_data_convert(dataframe):
    """
    :input: pandas dataframe
    :return:
    """
    try:
        print("processing...")

        # 1. add columns - 계산용 총매출, 순매출 (기존 컬럼 'amount_sales', 'amount_net_sales')
        # print("head(10)", dataframe.columns(0))

        # :: .csv 파일 읽어왔을때, 특정 columns name에 특수문자가 포함되어 있어, 전체 columns rename 처리
        # format - 21.04 version
        # format_columns_old = [
        #     "doc_no",
        #     "sales_month",
        #     "platform",
        #     "content_title",
        #     "author",
        #     "publisher",
        #     "cps",
        #     "count_free",
        #     "count_buy",
        #     "amount_buy",
        #     "count_rent",
        #     "amount_rent",
        #     "count_freepass",
        #     "amount_freepass",
        #     "count_cancel",
        #     "amount_cancel",
        #     "amount_sales",
        #     "rate_platform",
        #     "amount_net_sales",
        #     "rate_cp_org",
        #     "amount_cp_org",
        #     "amount_revenue_org",
        #     "content_type",
        # ]

        # format - 22.01
        format_columns = [
            "sales_code",
            "calc_month",
            "sales_month",
            "platform",
            "platform_detail",
            "platform_code",
            "platform_detail_code",
            "platform_locale",
            "platform_content_code",
            "content_title",
            "author",
            "publisher",
            "cps_org",
            "count_free",
            "count_buy",
            "amount_buy",
            "count_rent",
            "amount_rent",
            "count_freepass",
            "amount_freepass",
            "count_cancel",
            "amount_cancel",
            "payment_fee_correction",
            "amount_sales",
            "rate_platform",
            "amount_net_sales",
            "amount_sales_forien_fee_correction",
            "sum_rate_cps_org",
            "sum_amount_cps_org",
            "amount_revenue_org",
            "content_type",
            # "flag_issuance_tax_invoice",
            "content_code",
            "content_series",
            "content_series_code",
            # "description",
            # "month_calc",
            "sales_type",
            "advance_payment_mgr_code",  # 선수수익 관리코드
            "amount_calc_foreign_currency",
            "currency",
            "exchange_rate",
            "foriegn_in_rate",
            "description",
            "tax_correction",
            "flag_include_tax",
            "cp1_name",
            "cp1_code",
            "cp1_calc_code",
            "cp1_calc_std",
            "cp1_rate_calc",
            "cp1_except_advanced_rate_calc",
            "cp1_amount_calc",
            "cp1_type_calc",
            "cp1_description",
            "cp2_name",
            "cp2_code",
            "cp2_calc_code",
            "cp2_calc_std",
            "cp2_rate_calc",
            "cp2_except_advanced_rate_calc",
            "cp2_amount_calc",
            "cp2_type_calc",
            "cp2_description",
            "cp3_name",
            "cp3_code",
            "cp3_calc_code",
            "cp3_calc_std",
            "cp3_rate_calc",
            "cp3_except_advanced_rate_calc",
            "cp3_amount_calc",
            "cp3_type_calc",
            "cp3_description",
            "cp4_name",
            "cp4_code",
            "cp4_calc_code",
            "cp4_calc_std",
            "cp4_rate_calc",
            "cp4_except_advanced_rate_calc",
            "cp4_amount_calc",
            "cp4_type_calc",
            "cp4_description",
            # 이후 확인 필요
            "sales_apply_month",
            "sales_accounting_1",
            "bill_issuance",
            "sales_accounting_2",
            "accounting_proof_no",
            "accounting_date_verify"
            # ,
            # "platform_code",
            # "calc_revenue_std",
            # "flag_revenue",
            # "content_series_code",
            # "platform_details",
            # "sales_apply_month",
            # "accounting_platform",
            # "accounting_date_proof",
            # "accounting_date_verify",
        ]

        dataframe.columns = format_columns

        # @add columns - assign, 복제 컬럼
        # new_df = dataframe.assign(
        #     cps_org=lambda x: x["cps"],
        #     calc_amount_sales=lambda x: x["amount_sales"],
        #     calc_amount_net_sales=lambda x: x["amount_net_sales"],
        # )

        # @add colums - init
        new_df = dataframe.copy()
        new_df["calc_cp_id"] = 0
        new_df["calc_cp_name"] = ""
        new_df["calc_cp_code"] = ""
        new_df["calc_cp_calc_code"] = ""
        new_df["calc_cp_std"] = ""
        new_df["calc_cp_rate_calc"] = 0
        new_df["calc_cp_except_advanced_rate_calc"] = 0
        new_df["calc_cp_amount_calc"] = 0
        new_df["calc_cp_type_calc"] = ""
        new_df["calc_cp_description"] = ""

        # new_df["calc_content_id"] = ""
        # new_df["calc_cp_id"] = 0
        # new_df["calc_rate_std"] = ""  # 정산 기준 금액 - 총매출 or 순매출
        # new_df["calc_rate_std_code"] = ""  # 정산 기준 금액 - 총매출=sales or 순매출=net_sales
        # new_df["calc_rate_cp"] = 0
        # new_df["calc_amount_cp"] = 0
        # new_df["calc_flag_mg"] = 0
        # new_df["platform_id"] = 0

        # create default index
        # new_df = new_df.reset_index(drop=True)
        print("> org df rows = ", len(dataframe))
        print("> org df rows - new_df = ", len(new_df))
        # duplicated_df = new_df.query('cp.str.contains(",")')
        # print("#duplicated_df ", duplicated_df)
        # ret_df = new_df
        # # df_list = list(duplicated_df)

        # 22.03.15
        # df_final = pd.DataFrame()
        # df_final.columns = format_columns
        new_list = []
        for i in range(len(new_df)):
            row = new_df.iloc[i]

            # loop cp1 ~ cp4
            for cp_index in range(4):
                new_row = convert_row_data(row, cp_index + 1, i)
                # print("** new_row - cp_name>>", new_row["calc_cp_name"])
                # isinstance(cp_name, str)
                # if isinstance(new_row, object) and isinstance(new_row["calc_cp_name"], str):
                if not new_row.empty:
                    if isinstance(new_row["calc_cp_name"], str):
                        # print(" append row before")
                        # print(" appned row cp name", new_row["calc_cp_name"])
                        # print(" appned row cp name", type(new_row))
                        # dataframe.append(new_row, ignore_index=True, verify_integrity=True)
                        # df_final.append(new_row, ignore_index=True)
                        new_list.append(new_row)

                        # dataframe.append(new_row)
                        # print(" append row after - length", len(df_final))
                else:
                    # print(" \n XXXX new row cp_name >> ", new_row["calc_cp_name"])
                    print(" \n XXXX new row None >> ")

            # new_row = convert_row_data(row, 2, i)
            # if not new_row.empty:
            #     if isinstance(new_row["calc_cp_name"], str):
            #         new_list.append(new_row)
            # else:
            #     print(" \n XXXX new row None >> ")

        # for i in range(duplicated_df.shape[0] - 1, -1, -1):
        #     print("#duplicated_df >> i", i)
        #     print("#duplicated_df >> row - iloc ", duplicated_df.iloc[i])
        #     # print("#duplicated_df >> row - loc ", duplicated_df.loc[i])
        #     # for row in df_list:
        #     # cps = row["cp"].split(",")
        #     row = duplicated_df.iloc[i]
        #     cps = duplicated_df.iloc[i]["cp"].split(",")
        #     print("#duplicated_df >> cps", duplicated_df.iloc[i]["cp"])
        #     # @기존 row 삭제
        #     if len(cps) > 1:
        #         print(" --- drop row index=", i)
        #         print(" --- drop row =", row.name)
        #         new_df.drop(row.name, inplace=True)

        #     index = 0
        #     for i in cps:
        #         # print("** cp name - ", i.strip())
        #         # print("** index - ", index)

        #         # @todo index 확인
        #         row["cp"] = i.strip()
        #         if index > 0:
        #             row["amount_sales"] = 0
        #             row["amount_net_sales"] = 0

        #         new_df = new_df.append(row)
        #         index += 1

        print("\n\n*** new_list type >> ", type(new_list))
        df_final = pd.DataFrame(new_list)
        print("\n\n*** final rows >> ", len(df_final))
        return df_final

    except Exception as e:
        # print("[Error] @{0} : {1}".format(func_name(), e))
        print(traceback.format_exc())
