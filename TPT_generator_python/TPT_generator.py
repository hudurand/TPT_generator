import pandas as pd
import numpy as np
import re
from collections import OrderedDict
from pathlib import Path
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment

from .db_fetcher import TPT_Fetcher
from .cash_flow import Cash_Flow
from .scr_module import SCR_Module
from .data_bucket import Data_Bucket
from .processor import Data_Processor
from .constants import IN18, FIELDS

class TPT_Generator():
    """
    Class to generate a TPT report for a shareclass or all shareclass of a 
    subfund at a given date.

    It proceed by shareclass, generating the report for the shareclass 
    identified by the isin code held in ``self.shareclass_isin``.
    
    It relies on the Data_Bucket object to feed it the data required to fill
    the report and perform minor operation on the data to fill the columns 
    correctly. more complex computations are handled by the ``cash_flows``, 
    ``scr_module`` and ``processor`` modules.

    Args:
        date (pd.DateTime): reporting date
        client (str): client the report must be generated for
        shareclass_isin (str): isin code of the shareclass the report must \
            be generated for

    The TPT_report can be divided into a number of sections:

        - columns that are shareclass specific and will hold the same 
        information in all rows. This include the shareclass' related
        informations, as well as the subfund's, fund's and client's related 
        informations.
            - columns 1-11: shareclass related inforamtions.
            - columns 114-118: subfund related informations.
            - columns 119-124: fund related informations.
        
        - fixed instruments specific columns which will hold informations that
        depends on the instruments intrinsecs properties.
            - columns: 12-17; 20; 21; 32-95; 

        - varying instruments specific columns which will hold informations that
        depends on the specific investment made by the shareclass.

        - SCR columns which are computed by a dedicated module.
            - columns: 97-105b
    """

    def __init__(self,
                 date,
                 client=None,
                 shareclass_isin=None,
                 source_dir=None,
                 output_dir=None,
                 sym_adj=0):
        """
        Initialise report-specific attributes helper objects.
        """
        self.date = date
        self.client = client
        self.shareclass_isin = shareclass_isin
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)

        self.sym_adj = sym_adj
        self.fields = FIELDS
        self.IN18 = IN18
        self.cash_flows = Cash_Flow(self.date.strftime('%Y%m%d'))
        self.scr_module = SCR_Module()

        self.fetcher = TPT_Fetcher(self.date,
                                   self.client,
                                   self.shareclass_isin,
                                   self.source_dir)
        self.data_bucket = Data_Bucket(self.client,
                                       self.fetcher)
        self.processor = Data_Processor(self.data_bucket,
                                        self.fetcher)
        self.currency_rate = self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sc_curr") \
                             / self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sf_curr")
        
        self.create_empty_report()

    def generate(self):
        """
        Generate the report by calling the filling methods for all columns.
        """
        for field in self.fields:
            #print(f"fill_column_{field}...")
            getattr(self, f"fill_column_{field}")()

    def create_empty_report(self):
        """
        Create an empty pandas dataframe to hold the TPT report to generate.
        """
        Ncol = len(self.data_bucket.get_instruments().index)
        self.TPT_report = pd.DataFrame(index=range(Ncol), columns=self.fields.values(), dtype=object)
    
    def output_excel(self):
        """
        Saves the generated TPT report to an excel file using the AO template.
        """
        #root_path = Path('./data')
        template_file_name = 'AO_TPT_V5.0_Template.xlsx'
        output_file_name = f"AO_TPT_V5.0_{self.client}_{self.shareclass_isin}_{self.date}.xlsx"
        template = openpyxl.load_workbook(self.source_dir / template_file_name)
        report = template.get_sheet_by_name('Report')
        rows = dataframe_to_rows(self.TPT_report, index=False)

        column_map = {}
        for row_idx, row in enumerate(rows):
            if row_idx == 0:
                assert report.max_column == len(row), "Number of columns in report and template are different."
                for col_idx_pd, column_name in enumerate(row):
                    for i in range(len(row)):
                        if report.cell(row=1, column=i+1).value == column_name:
                            #print(column_name, report.cell(row=1, column=i+1).value)
                            column_map[col_idx_pd] = i+1
                            continue
                    assert col_idx_pd in column_map.keys(), f"Missing {column_name} in template"
                    assert report.cell(row=1, column=column_map[col_idx_pd]).value == row[col_idx_pd]
                
            else:
                for col_idx, value in enumerate(row):
                    if value == "nan":
                        report.cell(row=row_idx+1, column=column_map[col_idx], value="")
                        report.cell(row=row_idx+1, column=column_map[col_idx]).alignment = Alignment(horizontal='center')
                    else:
                        report.cell(row=row_idx+1, column=column_map[col_idx], value=value)
                        report.cell(row=row_idx+1, column=column_map[col_idx]).alignment = Alignment(horizontal='center')

        template.save(self.output_dir / output_file_name)

    def check_required(self, required_fields):
        """
        Check if the fields given as input are filled and fill them if necessary.
        """
        for field in required_fields:
            if self.TPT_report[self.fields[field]].isnull().values.all():
                #print(f"fill_column_{field}")
                getattr(self, f"fill_column_{field}")()

    def fill_instrument_info(self, info):
        """
        Abstracting class to fill fixed instruments speficific informations.
        """
        self.check_required(["14"])

        column = self.data_bucket.get_instruments_infos(info)
        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        self.TPT_report[info].update(
            column[info])
        self.TPT_report.reset_index(inplace=True)

    def fill_column_1(self):
        self.TPT_report.loc[:,self.fields["1"]] = self.fetcher.shareclass_isin
    
    def fill_column_2(self):
        self.TPT_report.loc[:,self.fields["2"]] = \
            int(self.data_bucket.get_shareclass_infos("type_tpt"))

    def fill_column_3(self):
        self.TPT_report.loc[:,self.fields["3"]] = \
            self.data_bucket.get_shareclass_infos("shareclass_name")
    
    def fill_column_4(self):
        self.TPT_report.loc[:,self.fields["4"]] = \
            self.data_bucket.get_shareclass_infos("shareclass_currency")

    def fill_column_5(self):
        self.TPT_report.loc[:,self.fields["5"]] = \
            self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sc_curr")

    def fill_column_6(self):
        self.TPT_report.loc[:,self.fields["6"]] = \
            self.data_bucket.get_shareclass_nav("nav_date")

    def fill_column_7(self):
        self.TPT_report.loc[:,self.fields["7"]] = \
            self.date.strftime('%Y-%m-%d')

    def fill_column_8(self):
        self.TPT_report.loc[:,self.fields["8"]] = \
            self.data_bucket.get_shareclass_nav("share_price")

    def fill_column_8b(self):
        self.TPT_report.loc[:,self.fields["8b"]] = \
            self.data_bucket.get_shareclass_nav("outstanding_shares")

    def fill_column_9(self):
        # sum of "XT72" CIC code of the instrument divided by shareclass MV
        self.check_required(["5", "12", "24"])
        
        TOTAL_CASH = self.TPT_report.loc[self.TPT_report[self.fields["12"]]=="XT72",
                                         self.fields["24"]].sum()
        #print(TOTAL_CASH)
        self.TPT_report[self.fields["9"]] = TOTAL_CASH / self.TPT_report[self.fields["5"]]
    
    def fill_column_10(self):
        self.check_required(["30", "91"])

        prod = self.TPT_report[self.fields["30"]] * self.TPT_report[self.fields["91"]]
        
        if prod.isnull().values.all():
            val = np.nan
        else:
            val = prod.sum()

        self.TPT_report[self.fields["10"]] = val
        
    def fill_column_11(self):
        self.TPT_report[self.fields["11"]] = "Y"

    def fill_column_12(self):
        self.fill_instrument_info(self.fields["12"])

    def fill_column_13(self):
        self.fill_instrument_info(self.fields["13"])

    def fill_column_14(self):
        column_14 = self.data_bucket.get_instruments_infos().index
        
        self.TPT_report[self.fields["14"]].update(column_14.to_numpy())

    def fill_column_15(self):
        self.fill_instrument_info(self.fields["15"])

    def fill_column_16(self):
        self.fill_instrument_info(self.fields["16"])

    def fill_column_17(self):
        self.fill_instrument_info(self.fields["17"])
    
    def fill_column_17b(self):
        pass

    def fill_column_18(self):
        self.check_required(["12", "14"])
        self.processor.compute_QN()
        #assert not self.TPT_report[self.fields["12"]].str.match("..22").any(), "some CIC code ends with 22 and are not supported yet"

        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        

        column_18 = pd.Series(index=self.TPT_report.index)
        column_18 = self.data_bucket.get_instruments("quantity_nominal") \
                    * self.data_bucket.get_distribution_weight(self.shareclass_isin)

        pattern = '|'.join([f"..{code}" for code in self.IN18])

        condition = (self.TPT_report[self.fields["12"]].str.match(pattern) \
                    | ((self.TPT_report[self.fields["12"]].str[2:] == "22") \
                    & (self.data_bucket.get_instruments("QN").round(0) == 1.0)))

        column_18.where(condition,
                        np.nan,
                        inplace=True)

        self.TPT_report[self.fields["18"]].update(column_18)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_19(self):
        self.check_required(["12", "14"])
        self.processor.compute_QN()

        #assert not self.TPT_report[self.fields["12"]].str.match("..22").any(), "some CIC code ends with 22 and are not supported yet"

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_19 = self.data_bucket.get_instruments("quantity_nominal") \
                      * self.data_bucket.get_distribution_weight(self.shareclass_isin)
        
        pattern = '|'.join([f"..{code}" for code in self.IN18])

        condition = ~((self.TPT_report[self.fields["12"]].str.match(pattern)) \
                    | ((self.TPT_report[self.fields["12"]].str[2:] == "22") \
                    & ~(self.data_bucket.get_instruments("QN").round(0) == 100.0)))

        column_19.where(condition,
                       np.nan,
                       inplace=True)

        self.TPT_report[self.fields["19"]].update(column_19)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_20(self):
        self.fill_instrument_info(self.fields["20"])

    def fill_column_21(self):
        self.fill_instrument_info(self.fields["21"])

    def fill_column_22(self):
        self.check_required(["14", "21"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_22 = self.data_bucket.get_instruments("market_and_accrued_asset") \
                    * self.data_bucket.get_distribution_weight(self.shareclass_isin)

        self.TPT_report[self.fields["22"]].update(column_22)
        self.TPT_report.reset_index(inplace=True)
        
    def fill_column_23(self):
        self.check_required(["14", "21"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_23 = self.data_bucket.get_instruments("market_asset") \
                    * self.data_bucket.get_distribution_weight(self.shareclass_isin)

        self.TPT_report[self.fields["23"]].update(column_23)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_24(self):
        self.check_required(["14"])

        column_24 = self.data_bucket.get_valuation_weight_vector(self.shareclass_isin) \
                    * self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sc_curr")

        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        self.TPT_report[self.fields["24"]].update(column_24)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_25(self):
        self.check_required(["14"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        
        column_25 = self.data_bucket.get_instruments("market_fund") \
                    * self.data_bucket.get_distribution_weight(self.shareclass_isin) \
                    * self.currency_rate

        self.TPT_report[self.fields["25"]].update(column_25.fillna(0))
        self.TPT_report.reset_index(inplace=True)

    def fill_column_26(self):
        self.check_required(["14"])

        column_26 = self.data_bucket.get_valuation_weight_vector(self.shareclass_isin)

        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        self.TPT_report[self.fields["26"]].update(column_26)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_27(self):
        self.check_required(["14", "23", "25", "28"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_27 = self.TPT_report[self.fields["28"]] \
                                    * self.data_bucket.get_instruments("market_asset") \
                                    / self.data_bucket.get_instruments("market_fund") \
                                    / self.currency_rate

        self.TPT_report[self.fields["27"]].update(column_27)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_28(self):
        self.check_required(["12", "18", "19", "20", "21", "23", "25", "61", "62", "71", "72"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        pattern = "..22|..A2|..B4"

        AI = self.data_bucket.get_instruments("market_and_accrued_asset").where(
             ~self.TPT_report[self.fields["12"]].str.match(pattern),
             self.data_bucket.get_instruments().apply(lambda x: self.compute_exception_AI(x), axis=1))

        column_28 = AI * self.data_bucket.get_distribution_weight(self.shareclass_isin) \
                       * self.data_bucket.get_instruments("market_fund") \
                       / self.data_bucket.get_instruments("market_asset") \
                       * self.currency_rate

        self.TPT_report[self.fields["28"]].update(column_28)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_29(self):
        pass

    def fill_column_30(self):
        self.check_required(["5", "28"])

        self.TPT_report[self.fields["30"]] = self.TPT_report[self.fields["28"]] \
                                             / self.TPT_report[self.fields["5"]]

    def fill_column_31(self):
        pass

    def fill_column_32(self):
        self.fill_instrument_info(self.fields["32"])

    def fill_column_33(self):
        self.fill_instrument_info(self.fields["33"])

    def fill_column_34(self):
        self.fill_instrument_info(self.fields["34"])

    def fill_column_35(self):
        self.fill_instrument_info(self.fields["35"])

    def fill_column_36(self):
        self.fill_instrument_info(self.fields["36"])

    def fill_column_37(self):
        self.fill_instrument_info(self.fields["37"])

    def fill_column_38(self):
        self.fill_instrument_info(self.fields["38"])

    def fill_column_39(self):
        self.fill_instrument_info(self.fields["39"])
        self.TPT_report[self.fields["39"]] = pd.to_datetime(self.TPT_report[self.fields["39"]]).dt.date
        self.TPT_report[self.fields["39"]] = self.TPT_report[self.fields["39"]].astype('str')
        self.TPT_report[self.fields["39"]].replace({"2099-12-31" : "9999-12-31"}, inplace=True)
        self.TPT_report[self.fields["39"]].replace("NaT", np.nan, inplace=True)

    def fill_column_40(self):
        self.fill_instrument_info(self.fields["40"])

    def fill_column_41(self):
        self.fill_instrument_info(self.fields["41"])

    def fill_column_42(self):
        self.fill_instrument_info(self.fields["42"])

    def fill_column_43(self):
        self.fill_instrument_info(self.fields["43"])
        self.TPT_report[self.fields["43"]] = pd.to_datetime(self.TPT_report[self.fields["43"]]).astype('str')
        self.TPT_report[self.fields["43"]].replace("NaT", np.nan, inplace=True)

    def fill_column_44(self):
        self.fill_instrument_info(self.fields["44"])

    def fill_column_45(self):
        self.fill_instrument_info(self.fields["45"])

    def fill_column_46(self):
        self.fill_instrument_info(self.fields["46"])

    def fill_column_47(self):
        self.fill_instrument_info(self.fields["47"])

    def fill_column_48(self):
        self.check_required(["14", "47"])

        self.TPT_report[self.fields["48"]] = 9
        self.TPT_report[self.fields["48"]].where(
            self.TPT_report[self.fields["47"]].isnull(),
            1,
            inplace=True)

    def fill_column_49(self):
        self.fill_instrument_info(self.fields["49"])

    def fill_column_50(self):
        self.fill_instrument_info(self.fields["50"])

    def fill_column_51(self):
        self.check_required(["50"])

        self.TPT_report[self.fields["51"]] = 9
        self.TPT_report[self.fields["51"]].where(
            self.TPT_report[self.fields["50"]].isnull(),
            1,
            inplace=True)

    def fill_column_52(self):
        self.fill_instrument_info(self.fields["52"])

    def fill_column_53(self):
        self.fill_instrument_info(self.fields["53"])

    def fill_column_54(self):
        self.fill_instrument_info(self.fields["54"])
        self.TPT_report[self.fields["54"]].where(
            self.TPT_report[self.fields["54"]].str.match("K..."),
            self.TPT_report[self.fields["54"]].str.get(0),
            inplace=True)

    def fill_column_55(self):
        self.fill_instrument_info(self.fields["55"])

    def fill_column_56(self):
        self.fill_instrument_info(self.fields["56"])

    def fill_column_57(self):
        self.fill_instrument_info(self.fields["57"])

    def fill_column_58(self):
        self.fill_instrument_info(self.fields["58"])

    def fill_column_58b(self):
        self.fill_instrument_info(self.fields["58b"])

    def fill_column_59(self):
        self.fill_instrument_info(self.fields["59"])

    def fill_column_60(self):
        self.fill_instrument_info(self.fields["60"])

    def fill_column_61(self):
        self.check_required(["16", "18", "19"])

        self.fill_instrument_info(self.fields["61"])

    def fill_column_62(self):
        self.fill_instrument_info(self.fields["62"])

    def fill_column_63(self):
        self.fill_instrument_info(self.fields["63"])

    def fill_column_64(self):
        self.fill_instrument_info(self.fields["64"])

    def fill_column_65(self):
        self.fill_instrument_info(self.fields["65"])
    
    def fill_column_67(self):
        self.fill_instrument_info(self.fields["67"])

    def fill_column_68(self):
        self.fill_instrument_info(self.fields["68"])

    def fill_column_69(self):
        self.fill_instrument_info(self.fields["69"])

    def fill_column_70(self):
        self.fill_instrument_info(self.fields["70"])

    def fill_column_71(self):
        self.fill_instrument_info(self.fields["71"])

    def fill_column_72(self):
        self.fill_instrument_info(self.fields["72"])

    def fill_column_73(self):
        self.fill_instrument_info(self.fields["73"])

    def fill_column_74(self):
        self.fill_instrument_info(self.fields["74"])

    def fill_column_75(self):
        self.fill_instrument_info(self.fields["75"])

    def fill_column_76(self):
        self.fill_instrument_info(self.fields["76"])

    def fill_column_77(self):
        self.fill_instrument_info(self.fields["77"])

    def fill_column_78(self):
        self.fill_instrument_info(self.fields["78"])

    def fill_column_79(self):
        self.fill_instrument_info(self.fields["79"])

    def fill_column_80(self):
        self.fill_instrument_info(self.fields["80"])

    def fill_column_81(self):
        self.fill_instrument_info(self.fields["81"])

    def fill_column_82(self):
        self.fill_instrument_info(self.fields["82"])

    def fill_column_83(self):
        self.fill_instrument_info(self.fields["83"])

    def fill_column_84(self):
        self.fill_instrument_info(self.fields["84"])

    def fill_column_85(self):
        self.fill_instrument_info(self.fields["85"])

    def fill_column_86(self):
        self.fill_instrument_info(self.fields["86"])

    def fill_column_87(self):
        self.fill_instrument_info(self.fields["87"])

    def fill_column_88(self):
        self.fill_instrument_info(self.fields["88"])

    def fill_column_89(self):
        self.fill_instrument_info(self.fields["89"])

    def fill_column_90(self):
        self.fill_instrument_info(self.fields["90"])

    def fill_column_91(self):
        self.fill_instrument_info(self.fields["91"])

    def fill_column_92(self):
        self.fill_instrument_info(self.fields["92"])

    def fill_column_93(self):
        self.fill_instrument_info(self.fields["93"])

    def fill_column_94(self):
        self.fill_instrument_info(self.fields["94"])

    def fill_column_94b(self):
        self.fill_instrument_info(self.fields["94b"])

    def fill_column_95(self):
        pass
    
    def fill_column_97(self):
        def compute_97(row):
            if self.TPT_report.loc[row.name, self.fields["12"]][2] in ["1", "2", "5"]:
                if self.cash_flows[row.name]["rfr"].sum() == 0:
                    return 0
                else:
                    return (1 - self.cash_flows[row.name]["up"].sum() \
                               / self.cash_flows[row.name]["rfr"].sum()) \
                               * self.TPT_report.loc[row.name, self.fields["26"]]
            return 0

        self.check_required(["7", "12", "14", "21", "26", "33", "38", "39", "43", "45"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        if not self.cash_flows.actualized:
            self.cash_flows.compute(self.TPT_report.join(self.data_bucket.get_instruments("quantity_nominal")))
        
        column_97 = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        
        column_97 = column_97.apply(lambda row: compute_97(row), axis=1)
                                    
        self.TPT_report[self.fields["97"]].update(column_97)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_98(self):
        def compute_98(row):
            if self.TPT_report.loc[row.name, self.fields["12"]][2] in ["1", "2", "5"]:
                if self.cash_flows[row.name]["rfr"].sum() == 0:
                    return 0
                else:
                    return (1 - self.cash_flows[row.name]["down"].sum() \
                               / self.cash_flows[row.name]["rfr"].sum()) \
                               * self.TPT_report.loc[row.name, self.fields["26"]]
            return 0

        self.check_required(["7", "12", "14", "21", "26", "33", "38", "39", "43", "45"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)
        
        if not self.cash_flows.actualized:
            self.cash_flows.compute(self.TPT_report.join(self.data_bucket.get_instruments("quantity_nominal")))
        
        column_98 = pd.DataFrame(index=self.TPT_report.index, columns=["col"])

        column_98 = column_98.apply(lambda row: compute_98(row), axis=1)
        
        self.TPT_report[self.fields["98"]].update(column_98)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_99(self):
        def shock_down_type1(row):
            if self.TPT_report.loc[row.name, self.fields["131"]] == "3L":
                return self.TPT_report.loc[row.name, self.fields["26"]] * (0.39 + self.sym_adj/100)

            elif self.TPT_report.loc[row.name, self.fields["12"]][2:] == "22":
                if not pd.isnull(self.TPT_report.loc[row.name, [self.fields["71"]]].iloc[0]):
                    main_ccy = self.TPT_report.loc[row.name, [self.fields["21"]]].iloc[0]
                    underlying_ccy = self.TPT_report.loc[row.name, [self.fields["71"]]].iloc[0]
            
                    if main_ccy != underlying_ccy:
                        EX = self.fetcher.ccy[main_ccy + underlying_ccy]
                    else: 
                        EX = 1

                else: 
                    EX = 1
                
                E = self.TPT_report.loc[row.name, self.fields["5"]]
                T = self.TPT_report.loc[row.name, self.fields["18"]]
                U = self.TPT_report.loc[row.name, self.fields["19"]]
                AA = self.TPT_report.loc[row.name, self.fields["23"]]
                AE = self.TPT_report.loc[row.name, self.fields["25"]]
                BS = self.TPT_report.loc[row.name, self.fields["61"]]
                CC = self.TPT_report.loc[row.name, self.fields["72"]]
                CX = self.TPT_report.loc[row.name, self.fields["93"]]

                return ((T + U) / BS * CC * EX * (CX / E) * (AE / AA)) * (0.39 + self.sym_adj/100)

            else:
                return 0

        self.check_required(["5", "12", "18", "19", "23", "25", "26", "61", "72", "93", "131"])
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_99 = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        column_99 = column_99.apply(lambda row: shock_down_type1(row), axis=1)

        self.TPT_report[self.fields["99"]].update(column_99)

        self.TPT_report.reset_index(inplace=True)
    
    def fill_column_100(self):
        def shock_down_type2(row):
            if self.TPT_report.loc[row.name, self.fields["131"]] in ["4", "3X"] or\
               self.TPT_report.loc[row.name, self.fields["12"]][2:] in ["B1", "B4"]:
                return self.TPT_report.loc[row.name, self.fields["30"]] * (0.49 + self.sym_adj/100)
            else:
                return 0

        self.check_required(["12", "30", "131"])
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_100 = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        column_100 = column_100.apply(lambda row: shock_down_type2(row), axis=1)

        self.TPT_report[self.fields["100"]].update(column_100)

        self.TPT_report.reset_index(inplace=True)
    
    def fill_column_101(self):
        pass

    def fill_column_102(self):
        def shock_down_spread(row):
            #print("\n", row.name)
            if self.TPT_report.loc[row.name, self.fields["131"]] in ["B", "E"]\
                or self.TPT_report.loc[row.name, self.fields["39"]] == "nan":
                return 0

            # Government bonds
            elif self.TPT_report.loc[row.name, self.fields["131"]] == "1":
                if self.TPT_report.loc[row.name, self.fields["53"]] == "1":
                    return 0
                else:
                    duration = self.TPT_report.loc[row.name, self.fields["90"]]
                    CQS = self.TPT_report.loc[row.name, self.fields["59"]]
                    shock = self.scr_module.spread_risk_parameter(3, duration, CQS)
            # Covered bonds
            elif self.TPT_report.loc[row.name, self.fields["55"]] == "C"\
                and self.TPT_report.loc[row.name, self.fields["59"]] < 2:
                duration = self.TPT_report.loc[row.name, self.fields["90"]]
                CQS = self.TPT_report.loc[row.name, self.fields["59"]]
                shock = self.scr_module.spread_risk_parameter(2, duration, CQS)
            # General bonds and loans
            elif self.TPT_report.loc[row.name, self.fields["12"]][2] in ["2", "8"]:
                duration = self.TPT_report.loc[row.name, self.fields["90"]]
                CQS = self.TPT_report.loc[row.name, self.fields["59"]]
                shock = self.scr_module.spread_risk_parameter(1, duration, CQS)
            else:
                return 0 

            return self.TPT_report.loc[row.name, self.fields["26"]] * shock
            
        self.check_required(["12", "14", "26", "53", "59", "90", "131"])
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_102 = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        column_102 = column_102.apply(lambda row: shock_down_spread(row), axis=1)

        self.TPT_report[self.fields["102"]].update(column_102)

        self.TPT_report.reset_index(inplace=True)
    
    def fill_column_103(self):
        pass
    
    def fill_column_104(self):
        pass

    def fill_column_105(self):
        pass

    def fill_column_105a(self):
        def shock_down_currency(row):
            fund_curr = self.TPT_report.loc[row.name, self.fields["4"]]
            quot_curr = self.TPT_report.loc[row.name, self.fields["21"]] 
            if quot_curr != fund_curr :
                return - self.TPT_report.loc[row.name, self.fields["30"]] * self.scr_module.currency_risk_parameter(fund_curr, quot_curr)
            else:
                return 0

        self.check_required(["4", "21", "30"])
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_105a = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        column_105a = column_105a.apply(lambda row: shock_down_currency(row), axis=1)

        self.TPT_report[self.fields["105a"]].update(column_105a)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_105b(self):
        def shock_up_currency(row):
            fund_curr = self.TPT_report.loc[row.name, self.fields["4"]]
            quot_curr = self.TPT_report.loc[row.name, self.fields["21"]] 
            if quot_curr != fund_curr :
                return self.TPT_report.loc[row.name, self.fields["30"]] * self.scr_module.currency_risk_parameter(fund_curr, quot_curr)
            else:
                return 0

        self.check_required(["4", "21", "30"])
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_105b = pd.DataFrame(index=self.TPT_report.index, columns=["col"])
        column_105b = column_105b.apply(lambda row: shock_up_currency(row), axis=1)

        self.TPT_report[self.fields["105b"]].update(column_105b)

        self.TPT_report.reset_index(inplace=True)


    def fill_column_106(self):
        pass

    def fill_column_107(self):
        pass

    def fill_column_108(self):
        pass

    def fill_column_109(self):
        pass

    def fill_column_110(self):
        pass

    def fill_column_111(self):
        pass
    
    def fill_column_112(self):
        pass

    def fill_column_113(self):
        pass

    def fill_column_114(self):
        self.check_required(["13"])

        self.TPT_report[self.fields["114"]] = \
            self.TPT_report[self.fields["13"]].where(
                self.TPT_report[self.fields["13"]] != 0)

    def fill_column_115(self):
        self.TPT_report.loc[:,self.fields["115"]] = \
            self.data_bucket.get_subfund_infos("subfund_lei")

    def fill_column_116(self):
        pass
        #self.TPT_report.loc[:,self.fields["116"]] = \
        #    self.data_bucket.get_subfund_infos("fund_issuer_code_type").astype('int64')

    def fill_column_117(self):
        self.TPT_report.loc[:,self.fields["117"]] = \
            self.data_bucket.get_subfund_infos("subfund_name")

    def fill_column_118(self):
        self.TPT_report.loc[:,self.fields["118"]] = \
            self.data_bucket.get_subfund_infos("subfund_nace")

    def fill_column_119(self):
        self.TPT_report.loc[:,self.fields["119"]] = \
            self.data_bucket.get_fund_infos("fund_issuer_group_code")

    def fill_column_120(self):
        pass
        #self.TPT_report.loc[:,self.fields["120"]] = \
        #    self.data_bucket.get_fund_infos("fund_issuer_group_code_type")

    def fill_column_121(self):
        self.TPT_report.loc[:,self.fields["121"]] = \
            self.data_bucket.get_fund_infos("fund_name")

    def fill_column_122(self):
        self.TPT_report.loc[:,self.fields["122"]] = \
            self.data_bucket.get_fund_infos("fund_country")

    def fill_column_123(self):
        self.TPT_report.loc[:,self.fields["123"]] = \
            self.data_bucket.get_subfund_infos("subfund_cic")

    def fill_column_123a(self):
        self.TPT_report.loc[:,self.fields["123a"]] = \
            self.data_bucket.get_fund_infos("depositary_country")

    def fill_column_124(self):
        self.check_required(["10"])

        self.TPT_report[self.fields["124"]] = self.TPT_report[self.fields["10"]]

    def fill_column_125(self):
        self.check_required(["14", "22"])
                
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_125 = self.data_bucket.get_instruments("accrued_asset") \
                     * self.TPT_report[self.fields["22"]] \
                     / self.data_bucket.get_instruments("market_and_accrued_asset")

        self.TPT_report[self.fields["125"]].update(column_125)
        self.TPT_report[self.fields["125"]].fillna(0, inplace=True)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_126(self):
        self.check_required(["14", "24"])
                
        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        column_126 = self.data_bucket.get_instruments("accrued_fund") \
                     * self.TPT_report[self.fields["24"]] \
                     / self.data_bucket.get_instruments("market_and_accrued_fund")

        self.TPT_report[self.fields["126"]].update(column_126)
        self.TPT_report[self.fields["126"]].fillna(0, inplace=True)

        self.TPT_report.reset_index(inplace=True)

    def fill_column_127(self):
        self.fill_instrument_info(self.fields["127"])
        self.TPT_report[self.fields["127"]].replace("-", np.nan, inplace=True)

    def fill_column_128(self):
        self.fill_instrument_info(self.fields["128"])
        self.TPT_report[self.fields["128"]].replace("-", np.nan, inplace=True)
    
    def fill_column_129(self):
        pass

    def fill_column_130(self):
        pass

    def fill_column_131(self):
        def select_value(row):
            if row[self.fields["12"]][2] == "3":
                if row[self.fields["12"]][:2] == "XL":
                    return "3X"
                else:
                    return "3L"
            if row[self.fields["12"]][2] in ["7", "8", "0"] and\
               row[self.fields["24"]] < 0:
                return "L"
            
            return row[self.fields["12"]][2]

        self.check_required(["12", "14", "24"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        self.TPT_report[self.fields["131"]] = self.TPT_report.apply(lambda row: select_value(row), axis=1)
        self.TPT_report.reset_index(inplace=True)

    def fill_column_132(self):
        pass

    def fill_column_133(self):
        self.TPT_report.loc[:,self.fields["133"]] = \
            self.data_bucket.get_fund_infos("depositary_name")

    def fill_column_134(self):
        pass

    def fill_column_135(self):
        pass

    def fill_column_136(self):
        pass

    def fill_column_137(self):
        def select_value(row):
            if row[self.fields["15"]] == 1:
                return np.nan
            if row[self.fields["12"]][2] == "1":
                return 10
            if row[self.fields["12"]][2] == "6":
                return 6
            if row[self.fields["12"]][2] == "7":
                if row[self.fields["54"]][0] == "K" or\
                   row[self.fields["54"]][0] == "O":
                    return 12
                else:
                    return 13
            if row[self.fields["54"]] == "K6411":
                return 1
            if row[self.fields["54"]] == "K6419":
                return 2
            if row[self.fields["54"]] == "K6630":
                if row[self.fields["12"]][2:] == "43":
                    return 3
                else:
                    return 4
            if row[self.fields["54"]] == "K6619" or\
               row[self.fields["54"]][:3] == "K649":
                return 5
            if row[self.fields["54"]][:3] == "K651" or\
               row[self.fields["54"]][:3] == "K652":
                return 7
            if row[self.fields["54"]][:3] == "K653":
                return 8
            if row[self.fields["54"]][0] == "T":
                return 
            if row[self.fields["54"]][0] not in ["K","T"]:
                return 9

        self.check_required(["12", "14", "15", "54"])

        self.TPT_report.set_index([self.fields["14"]], inplace=True)

        self.TPT_report[self.fields["137"]] = self.TPT_report.apply(lambda row: select_value(row), axis=1)
        
        self.TPT_report.reset_index(inplace=True)

    def fill_column_1000(self):
        self.TPT_report[self.fields["1000"]] = "V5.0"
    
    def compute_exception_AI(self, row):
        CIC = self.TPT_report.loc[row.name, [self.fields["12"]]].iloc[0]

        V = (self.data_bucket.get_instruments("quantity_nominal") \
            * self.data_bucket.get_distribution_weight(self.shareclass_isin)).loc[row.name]

        CC = self.TPT_report.loc[row.name, [self.fields["72"]]].iloc[0] if \
            not pd.isnull(self.TPT_report.loc[row.name, [self.fields["72"]]]).iloc[0] else 1
        
        W = self.TPT_report.loc[row.name, [self.fields["20"]]].iloc[0]
        BS = self.TPT_report.loc[row.name, [self.fields["61"]]].iloc[0]

        if not pd.isnull(self.TPT_report.loc[row.name, [self.fields["71"]]].iloc[0]):
            main_ccy = self.TPT_report.loc[row.name, [self.fields["21"]]].iloc[0]
            underlying_ccy = self.TPT_report.loc[row.name, [self.fields["71"]]].iloc[0]
        
            if main_ccy != underlying_ccy:
                EX = self.fetcher.ccy[main_ccy + underlying_ccy]
            else: 
                EX = 1
        
        else: 
            EX = 1
    
        if CIC[2:] == "22":
            CX = self.TPT_report.loc[row.name, [self.fields["73"]]].iloc[0] \
                if not pd.isnull(self.TPT_report.loc[row.name, [self.fields["73"]]].iloc[0]) else 1

            AI = max(V / BS * CC * EX * CX, 
                     row["market_and_accrued_asset"])
        
        elif CIC[2:] == "A2":
            AI = min(V * W/100 * CC * EX, 
                     row["market_and_accrued_asset"])
            
        elif CIC[2:] == "B4":
            BT = self.TPT_report.loc[row.name, [self.fields["62"]]].iloc[0]\
                 if not pd.isnull(self.TPT_report.loc[row.name, [self.fields["62"]]].iloc[0]) else 1
            AI = max(V * BT * (CC-BS) * EX,
                     row["market_and_accrued_asset"])

        else:
            AI = row["market_and_accrued_asset"]

        return AI
