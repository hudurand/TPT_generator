import pandas as pd
import numpy as np
import re
from collections import OrderedDict
from pathlib import Path
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment

from .db_fetcher import TPT_Fetcher
from .scr_module import SCR_Module
from .data_bucket import Data_Bucket
from .processor import Data_Processor
from .constants import IN18, FIELDS

class TPT_Generator():
    """
    Factory object to generate a TPT report for a shareclass or all shareclass
    of a subfund at a given date.
   
    It relies on the Data_Bucket object to feed it the data required to fill
    the report (and perform minor operation on the data?) to fill the columns
    correctly.

    Args:
        date (pd.DateTime): reporting date
        client (str): client the report must be generated for
        shareclass_isin (str): isin code of the shareclass the report must \
            be generated for.
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

        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        
        self.sym_adj = sym_adj
        self.fields = FIELDS
        self.IN18 = IN18

        self.data_bucket = Data_Bucket(date,
                                       client,
                                       shareclass_isin,
                                       source_dir)

        self.currency_rate = self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sc_curr") \
                             / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr")
        
        self.create_empty_report()

    def __repr__(self):
        return f"""
    Reporting date:          {self.data_bucket.date}
    Client:                  {self.data_bucket.client}
    Shareclass isin:         {self.data_bucket.shareclass_isin} 
        """

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
        self.TPT_report = pd.DataFrame(index=self.data_bucket.get_instruments().index, columns=self.fields.values(), dtype=object)
    
    def output_excel(self):
        """
        Saves the generated TPT report to an excel file using the AO template.
        """
        #root_path = Path('./data')
        client = self.data_bucket.client
        isin = self.data_bucket.shareclass_isin
        date = self.data_bucket.date
        template_file_name = 'AO_TPT_V5.0_Template.xlsx'
        output_file_name = f"AO_TPT_V5.0_{client}_{isin}_{date}.xlsx"
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

        self.TPT_report[info].update(self.data_bucket.get_instruments_infos(info=info))
    
    
    def fill_SCR(self, submodule):
        """
        Abstracting class to fill fixed SCR computed values.
        """

        self.TPT_report[submodule].update(self.data_bucket.get_SCR_results(submodule))

    def fill_column_1(self):
        """
        Fill column "1_Portfolio identifying data".
        
        Code isin of the shareclass, provided by config.
        """
        self.TPT_report.loc[:,self.fields["1"]] = self.data_bucket.shareclass_isin
    
    def fill_column_2(self):
        """
        Fill column "2_Type of identification code for the fund share or portfolio".

        Codification chosen to identify the shareclass.
        """
        self.TPT_report.loc[:,self.fields["2"]] = \
            int(self.data_bucket.get_shareclass_infos("type_tpt"))

    def fill_column_3(self):
        """
        Fill column "3_Portfolio_name".

        Name of the shareclass, obtained from database.
        """
        self.TPT_report.loc[:,self.fields["3"]] = \
            self.data_bucket.get_shareclass_infos("shareclass_name")
    
    def fill_column_4(self):
        """
        Fill column "4_Portfolio_currency_(B)".

        Valuation currency of the portfolio.
        """
        self.TPT_report.loc[:,self.fields["4"]] = \
            self.data_bucket.get_shareclass_infos("shareclass_currency")

    def fill_column_5(self):
        self.TPT_report.loc[:,self.fields["5"]] = \
            self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sc_curr")

    def fill_column_6(self):
        self.TPT_report.loc[:,self.fields["6"]] = \
            self.data_bucket.get_shareclass_nav("nav_date")

    def fill_column_7(self):
        self.TPT_report.loc[:,self.fields["7"]] = \
            self.data_bucket.date.strftime('%Y-%m-%d')

    def fill_column_8(self):
        self.TPT_report.loc[:,self.fields["8"]] = \
            self.data_bucket.get_shareclass_nav("share_price")

    def fill_column_8b(self):
        self.TPT_report.loc[:,self.fields["8b"]] = \
            self.data_bucket.get_shareclass_nav("outstanding_shares")

    def fill_column_9(self):
        # sum of "XT72" CIC code of the instrument divided by shareclass MV
        # TODO: replace by running bucket at init
        _ = self.data_bucket.get_processing_data("distribution")
        self.TPT_report[self.fields["9"]] = self.data_bucket.get_shareclass_infos(info="cash") \
            / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sc_curr")
    
    def fill_column_10(self):

        #prod = self.TPT_report[self.fields["30"]] * self.TPT_report[self.fields["91"]]
        product = self.data_bucket.get_processing_data("ME") \
                  / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr") \
                  * self.data_bucket.get_instruments_infos(self.fields["91"])
        
        if product.isnull().values.all():
            val = np.nan
        else:
            val = product.sum()

        self.TPT_report[self.fields["10"]] = val
        
    def fill_column_11(self):
        self.TPT_report[self.fields["11"]] = "Y"

    def fill_column_12(self):
        self.fill_instrument_info(self.fields["12"])

    def fill_column_13(self):
        self.fill_instrument_info(self.fields["13"])

    def fill_column_14(self):
        #column_14 = self.data_bucket.get_instruments().index
        
        self.TPT_report[self.fields["14"]] = self.TPT_report.index

    def fill_column_15(self):
        self.fill_instrument_info(self.fields["15"])

    def fill_column_16(self):
        self.fill_instrument_info(self.fields["16"])

    def fill_column_17(self):
        self.fill_instrument_info(self.fields["17"])
    
    def fill_column_17b(self):
        pass

    def fill_column_18(self):
        #assert not self.TPT_report[self.fields["12"]].str.match("..22").any(), "some CIC code ends with 22 and are not supported yet"
        column_18 = pd.Series(index=self.TPT_report.index)
        column_18 = self.data_bucket.get_instruments("quantity_nominal") \
                    * self.data_bucket.get_distribution_weight()

        pattern = '|'.join([f"..{code}" for code in self.IN18])

        condition = (self.data_bucket.get_processing_data(self.fields["12"]).str.match(pattern) \
                    | ((self.data_bucket.get_processing_data(self.fields["12"]).str[2:] == "22") \
                    & (self.data_bucket.get_processing_data("QN").round(0) == 1.0)))

        column_18.where(condition,
                        np.nan,
                        inplace=True)

        self.TPT_report[self.fields["18"]].update(column_18)

    def fill_column_19(self):
        #assert not self.TPT_report[self.fields["12"]].str.match("..22").any(), "some CIC code ends with 22 and are not supported yet"

        column_19 = self.data_bucket.get_instruments("quantity_nominal") \
                      * self.data_bucket.get_distribution_weight()
        
        pattern = '|'.join([f"..{code}" for code in self.IN18])

        condition = ~((self.data_bucket.get_processing_data(self.fields["12"]).str.match(pattern)) \
                    | ((self.data_bucket.get_processing_data(self.fields["12"]).str[2:] == "22") \
                    & ~(self.data_bucket.get_processing_data("QN").round(0) == 100.0)))

        column_19.where(condition,
                       np.nan,
                       inplace=True)

        self.TPT_report[self.fields["19"]].update(column_19)

    def fill_column_20(self):
        self.fill_instrument_info(self.fields["20"])

    def fill_column_21(self):
        self.fill_instrument_info(self.fields["21"])

    def fill_column_22(self):

        column_22 = self.data_bucket.get_instruments("market_and_accrued_asset") \
                    * self.data_bucket.get_distribution_weight()

        self.TPT_report[self.fields["22"]].update(column_22)
        
    def fill_column_23(self):
        column_23 = self.data_bucket.get_instruments("market_asset") \
                    * self.data_bucket.get_distribution_weight()

        self.TPT_report[self.fields["23"]].update(column_23)

    def fill_column_24(self):
        column_24 = self.data_bucket.get_processing_data("valuation weight") \
                    * self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sc_curr")

        self.TPT_report[self.fields["24"]].update(column_24)

    def fill_column_25(self):
        
        column_25 = self.data_bucket.get_instruments("market_fund") \
                    * self.data_bucket.get_processing_data("distribution weight") \
                    * self.currency_rate

        self.TPT_report[self.fields["25"]].update(column_25.fillna(0))

    def fill_column_26(self):
        column_26 = self.data_bucket.get_processing_data("valuation weight")

        self.TPT_report[self.fields["26"]].update(column_26)

    def fill_column_27(self):
        column_27 = self.data_bucket.get_processing_data("ME") \
                    * self.data_bucket.get_instruments("market_asset") \
                    / self.data_bucket.get_instruments("market_fund")

        self.TPT_report[self.fields["27"]].update(column_27)

    def fill_column_28(self):
        #column_28 = self.data_bucket.get_processing_data("ME") \
        #            * self.data_bucket.get_instruments("market_fund") \
        #            / self.data_bucket.get_instruments("market_asset")

        self.TPT_report[self.fields["28"]] = self.data_bucket.get_processing_data("ME") \
                                             * self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sc_curr") \
                                             / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr")


    def fill_column_29(self):
        pass

    def fill_column_30(self):
#        self.TPT_report[self.fields["30"]] = self.TPT_report[self.fields["28"]] \
#                                             / self.TPT_report[self.fields["5"]]
        self.TPT_report[self.fields["30"]] = self.data_bucket.get_processing_data("ME") \
                                             / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr")

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
        self.check_required(["47"])

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
        self.fill_instrument_info(self.fields["61"])
        #column_61 = self.data_bucket.get_processing_data("61_Strike price")

        #self.TPT_report[self.fields["61"]].update(column_61)

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
        self.fill_SCR(self.fields["97"])

    def fill_column_98(self):
        self.fill_SCR(self.fields["98"])

    def fill_column_99(self):
        self.fill_SCR(self.fields["99"])

    def fill_column_100(self):
        self.fill_SCR(self.fields["100"])
    
    def fill_column_101(self):
        pass

    def fill_column_102(self):
        self.fill_SCR(self.fields["102"])
    
    def fill_column_103(self):
        pass
    
    def fill_column_104(self):
        pass

    def fill_column_105(self):
        pass

    def fill_column_105a(self):
        self.fill_SCR(self.fields["105a"])

    def fill_column_105b(self):
        self.fill_SCR(self.fields["105b"])

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
        
        self.check_required(["115"])

        self.TPT_report[self.fields["116"]] = 9
        self.TPT_report[self.fields["116"]].where(
            self.TPT_report[self.fields["115"]].isnull(),
            1,
            inplace=True)

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
        
        self.check_required(["119"])

        self.TPT_report[self.fields["120"]] = 9
        self.TPT_report[self.fields["120"]].where(
            self.TPT_report[self.fields["119"]].isnull(),
            1,
            inplace=True)
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
        column_125 = self.data_bucket.get_instruments("accrued_asset") \
                     * self.data_bucket.get_instruments("market_and_accrued_asset") \
                     * self.data_bucket.get_distribution_weight() \
                     / self.data_bucket.get_instruments("market_and_accrued_asset")

        self.TPT_report[self.fields["125"]].update(column_125)
        self.TPT_report[self.fields["125"]].fillna(0, inplace=True)

    def fill_column_126(self):
        column_126 = self.data_bucket.get_instruments("accrued_fund") \
                     * self.data_bucket.get_processing_data("valuation weight") \
                    * self.data_bucket.get_shareclass_nav("shareclass_total_net_asset_sc_curr") \
                     / self.data_bucket.get_instruments("market_and_accrued_fund") 

        self.TPT_report[self.fields["126"]].update(column_126)
        self.TPT_report[self.fields["126"]].fillna(0, inplace=True)

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
        column_131 = self.data_bucket.get_processing_data(self.fields["131"])

        self.TPT_report[self.fields["131"]].update(column_131)

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
        self.fill_instrument_info(self.fields["137"])

    def fill_column_1000(self):
        self.TPT_report[self.fields["1000"]] = "V5.0"