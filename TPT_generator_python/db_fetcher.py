import pyodbc
from pathlib import Path
import pandas as pd
import numpy as np
from timeit import default_timer as timer

class TPT_Fetcher():
    """
    fecther class to fecth data associated 
    to a client or shareclass from database
    required to fill the TPT report
    """

    def __init__(self,
                 date,
                 client=None,
                 shareclass_isin=None,
                 ):

        """
        initialise db connector and shareclass-specific infos
        """
        self.connector = pyodbc.connect('driver={SQL Server};'
                                        'Server=DESKTOP-RGN6M86;'
                                        'Database=intranet;'
                                        'Trusted_Connection=yes;')

        self.shareclass_isin = shareclass_isin
        self.client = client
        self.date = date
        self.shareclass_infos = None
        self.shareclass_nav = None
        self.subfund_infos = None
        self.fund_infos = None
        self.instruments = None
        self.instruments_infos = None
        self.bloomberg_infos = None
        self.portfolio_NAVs= None

    def open_portfolio(self):
        portfolio_file_name = "AO_Portfolio Valuation BIL Invest_20201231.xlsx"
        root_data_path = Path('./data')
        portfolio_file_path = root_data_path / portfolio_file_name
        self.portfolio_NAVs = pd.read_excel(portfolio_file_path, sheet_name="NAV")

    def fetch(self):
        self.fetch_shareclass_infos()
        self.fetch_subfund_infos()
        self.fetch_fund_infos()
        self.fetch_shareclass_nav()
        self.fetch_instruments()
        self.fetch_instruments_infos()

    def fetch_shareclass_infos(self):
        self.shareclass_infos = pd.read_sql_query('SELECT '
                                                  'id, '
                                                  'code_isin, '
                                                  'shareclass, '
                                                  'shareclass_currency, '
                                                  'shareclass_name, '
                                                  'id_subfund, '
                                                  'type_tpt '
                                                  'FROM intranet.dbo.shareclass '
                                                  f"WHERE code_isin='{self.shareclass_isin}'",
                                                  self.connector)

    def get_shareclass_infos(self):
        if self.shareclass_infos is None:
            self.fetch_shareclass_infos()

        return self.shareclass_infos

    def fetch_subfund_infos(self):
        subfund_id = self.get_shareclass_infos()["id_subfund"].iloc[0]
        self.subfund_infos = pd.read_sql_query('SELECT '
                                               'fund_issuer_code,'
                                               'fund_issuer_code_type, '
                                               'subfund_name, '
                                               'subfund_code, '
                                               'fund_issuer_sector, '
                                               'fund_cic_code, '
                                               'id_fund, '
                                               'subfund_currency '
                                               'FROM intranet.dbo.subfund '
                                               f"WHERE id='{subfund_id}'", 
                                               self.connector)

    def get_subfund_infos(self):
        if self.subfund_infos is None:
            self.fetch_subfund_infos()

        return self.subfund_infos
                        
    def fetch_fund_infos(self):
        fund_id = self.get_subfund_infos()["id_fund"].iloc[0]
        self.fund_infos = pd.read_sql_query('SELECT '
                                            'fund_issuer_group_code, '
                                            'fund_issuer_group_code_type, '
                                            'fund_issuer_group_name, '
                                            'fund_issuer_country, '
                                            'fund_custodian_country, '
                                            'custodian_name '
                                            'FROM intranet.dbo.fund '
                                            f"WHERE id='{fund_id}'", 
                                            self.connector)
        
    def get_fund_infos(self):
        if self.fund_infos is None:
            self.fetch_fund_infos()

        return self.fund_infos

    def fetch_shareclass_nav(self):
        if self.portfolio_NAVs is None:
            self.open_portfolio()

        sc_id = self.get_shareclass_infos()["id"].iloc[0]
        sc_curr = self.get_shareclass_infos()['shareclass_currency'].iloc[0]
        sf_curr = self.get_subfund_infos()['subfund_currency'].iloc[0]
        self.shareclass_nav = pd.read_sql_query('SELECT '
                                                'nav_date, '
                                                'share_price, '
                                                'outstanding_shares '
                                                'FROM intranet.dbo.nav '
                                                f"WHERE id_shareclass='{sc_id}' "
                                                f"AND nav_date='{self.date}' ",
                                                self.connector)
        
        self.shareclass_nav['shareclass_total_net_asset_sc_curr'] = pd.read_sql_query('SELECT '
                                                                                      'shareclass_total_net_asset '
                                                                                      'FROM intranet.dbo.nav '
                                                                                      f"WHERE id_shareclass='{sc_id}' "
                                                                                      f"AND nav_date='{self.date}' "
                                                                                      f"AND nav_currency='{sc_curr}'",
                                                                                      self.connector)
        self.shareclass_nav['shareclass_total_net_asset_sf_curr'] = pd.read_sql_query('SELECT '
                                                                                      'shareclass_total_net_asset '
                                                                                      'FROM intranet.dbo.nav '
                                                                                      f"WHERE id_shareclass='{sc_id}' "
                                                                                      f"AND nav_date='{self.date}' "
                                                                                      f"AND nav_currency='{sf_curr}'",
                                                                                      self.connector)    

        NAV = self.portfolio_NAVs.loc[self.portfolio_NAVs["ISIN"]==self.shareclass_isin]

        self.shareclass_nav["Hedged"] = NAV["Hedged sc"].iloc[0]

    def get_shareclass_nav(self):
        if self.shareclass_nav is None:
            self.fetch_shareclass_nav()

        return self.shareclass_nav
    
    def fetch_instruments(self):
        sc_indicator = self.get_shareclass_infos()["shareclass"].iloc[0]
        subfund_id = self.shareclass_infos["id_subfund"].iloc[0]
        infos = ', '.join(["asset_id_string",
                           "hedge_indicator",
                           "asset_name",
                           "asset_type_3",
                           "asset_currency",
                           "quantity_nominal",
                           "market_asset",
                           "market_fund",
                           "accrued_asset",
                           "accrued_fund",
                           "market_and_accrued_fund",
                           "market_and_accrued_asset",
                           "contract_number",
                           "maturity_date"])

        query = f"SELECT {infos} FROM intranet.dbo.portfolio_data d "\
                +"INNER JOIN portfolio p ON p.id = d.id_portfolio "\
                +f"WHERE id_subfund='{subfund_id}' "\
                +f"AND portfolio_date='{self.date}' " \
                +f"AND (hedge_indicator='{sc_indicator}' "\
                +"OR hedge_indicator IS NULL)"

        self.instruments = pd.read_sql_query(query,
                                             self.connector)

        self.instruments.set_index("asset_id_string", inplace=True)

        #assert self.instruments.loc["564_1", "market_and_accrued_fund"] == self.instruments.loc["564_1", "market_fund"]]

        if self.instruments.index.duplicated().any():
            fused = pd.DataFrame(columns=["quantity_nominal"
                                          "market_and_accrued_fund",
                                          "market_fund",
                                          "accrued_fund",
                                          "market_and_accrued_asset",
                                          #"market_asset",
                                          "accrued_asset"])
            fused["quantity_nominal"] = self.instruments.groupby("asset_id_string")["quantity_nominal"].sum()
            fused["market_and_accrued_fund"] = self.instruments.groupby("asset_id_string")["market_and_accrued_fund"].sum()
            fused["market_fund"] = self.instruments.groupby("asset_id_string")["market_fund"].sum()
            fused["accrued_fund"] = self.instruments.groupby("asset_id_string")["accrued_fund"].sum()
            fused["market_and_accrued_asset"] = self.instruments.groupby("asset_id_string")["market_and_accrued_asset"].sum()
            fused["market_asset"] = self.instruments.groupby("asset_id_string")["market_asset"].sum()
            fused["accrued_asset"] = self.instruments.groupby("asset_id_string")["accrued_asset"].sum()

            self.instruments = self.instruments[~self.instruments.index.duplicated()]
            self.instruments["quantity_nominal"].update(fused["quantity_nominal"])
            self.instruments["market_and_accrued_fund"].update(fused["market_and_accrued_fund"])
            self.instruments["market_fund"].update(fused["market_fund"])
            self.instruments["accrued_fund"].update(fused["accrued_fund"])
            self.instruments["market_and_accrued_asset"].update(fused["market_and_accrued_asset"])
            self.instruments["market_asset"].update(fused["market_asset"])
            self.instruments["accrued_asset"].update(fused["accrued_asset"])

        self.instruments["accrued_fund"].fillna(0, inplace=True)
        self.instruments["accrued_asset"].fillna(0, inplace=True)
        self.instruments["market_fund"] = self.instruments["market_and_accrued_fund"] - self.instruments["accrued_fund"]
        self.instruments["market_asset"] = self.instruments["market_and_accrued_asset"] - self.instruments["accrued_asset"]
        
    def get_instruments(self):
        if self.instruments is None:
            self.fetch_instruments()
        return self.instruments

    def fetch_instruments_infos(self):

        #######################################################################
        ##################### GET MISSING DATA FROM EXCEL #####################
        #######################################################################
        AODB_file_name = "AO Data Base v0.8.xlsx"
        BBG_file_name = "AO_Bloomberg_Template_SII.xlsx"
        root_data_path = Path('./data')
        AODB_file_path = root_data_path / AODB_file_name
        BBG_file_path = root_data_path / BBG_file_name
        
        print("loading missing data from excel")
        start = timer()
        print("loading AODB cash sheet")
        start_AODB = timer()
        self.AODB_CASH = pd.read_excel(AODB_file_path, sheet_name="Cash", skiprows=1)
        end_AODB = timer()
        print(f"loaded AODB cash sheet in {end_AODB - start_AODB} seconds.")
        print("loading BBG hard copy sheet")
        start_BBG = timer()
        BBG = pd.read_excel(BBG_file_path, sheet_name="Hard Copy", skiprows=2)
        end_BBG = timer()
        print(f"loaded BBG hard copy sheet in {end_BBG - start_BBG} seconds.")
        print("loading ccy sheet")
        start_CCY = timer()
        ccy = pd.read_excel(BBG_file_path, sheet_name="ccy", usecols="E,F", names=["pair", "rate"])
        self.ccy = ccy.set_index("pair")["rate"]
        end = timer()
        print(f"loaded ccy sheet in {end - start_CCY} seconds.")
        print(f"loaded missing data in {end - start} seconds.")
        bloomberg_infos = BBG.loc[BBG["ISIN"].isin(self.get_instruments().index),
                                  ["ISIN",
                                   "YAS_RISK",
                                   "YAS_MOD_DUR",
                                   "CV_MODEL_DELTA_S",
                                   "DUR_ADJ_MTY_MID",
                                   "cv_model_gamma_v",
                                   "CV_MODEL_vega",
                                   "cv_model_cnvs_prem",
                                   "Bond floor"]]
        
        bloomberg_infos.rename(columns={"YAS_RISK" : "92_Credit sensitivity",
                                        "YAS_MOD_DUR" : "91_Modified duration to next option exercise date",
                                        "CV_MODEL_DELTA_S" : "93_Sensitivity to underlying asset price (delta)",
                                        "DUR_ADJ_MTY_MID" : "90_Modified Duration to maturity date",
                                        "cv_model_gamma_v" : "94_Convexity / gamma for derivatives",
                                        "CV_MODEL_vega" : "94b_Vega",
                                        "cv_model_cnvs_prem" : "128_Option premium (convertible instrument only)",
                                        "Bond floor" : "127_Bond Floor (convertible instrument only)"},
                               inplace=True)

        #######################################################################
        
        instruments_infos_dict = {
            "12_CIC code of the instrument" : "cic",
            "13_Economic zone of the quotation place" : "economic_zone_of_the_quotation_place",
            "14_Identification code of the financial instrument" : "identification_code",
            "15_Type of identification code for the instrument" : "type_identification_code_for_the_instrument",
            "20_Contract size for derivatives" : "contract_size_for_derivatives", 
            "21_Quotation currency (A)" : "quotation_currency",
            "32_Interest rate type" : "interest_rate_type",
            "33_Coupon rate" : "coupon_rate",
            "34_Interest rate reference identification" : "interest_rate_reference_identification",
            "35_Identification type for interest rate index" : "identification_type_for_interest_rate_index",
            "36_Interest rate index name" : "interest_rate_index_name",
            "37_Interest rate Margin" : "interest_rate_margin",
            "38_Coupon payment frequency" : "coupon_payment_frequency",
            "40_Redemption type" : "redemption_type",
            "41_Redemption rate" : "redemption_rate",
            "42_Callable / putable" : "callable_puttable",
            "43_Call / put date" : "call_put_date",
            "44_Issuer / bearer option exercise" : "issuer_bearer_option_exercise",
            "45_Strike price for embedded (call/put) options" : "strike_price_for_embedded_callput_options",
            "46_Issuer name" : "issuer_name",
            "47_Issuer identification code" : "issuer_identification_code",
            "49_Name of the group of the issuer" : "name_of_the_group_of_the_issuer",
            "50_Identification of the group" : "identification_of_the_group",
            "52_Issuer country" : "iso_code_2",
            "53_Issuer economic area" : "Geographic",
            "54_Economic sector" : "economic_sector",
            "55_Covered / not covered" : "covered_not_covered",
            "56_Securitisation" : "securitisation",
            "57_Explicit guarantee by the country of issue" : "explicit_guarantee_by_the_country_of_issue",
            "58_Subordinated debt" : "subordinated_debt",
            "58b_Nature of the TRANCHE" : "nature_of_the_tranche",
            "59_Credit quality step" : "credit_quality_step",
            "60_Call / Put / Cap / Floor" : "call_put_cap_floor",
            "61_Strike price" : "strike_price",
            "62_Conversion factor (convertibles) / concordance factor / parity (options)" : "conversion_factor_convertibles_concordance_factor_parity_options",
            "63_Effective Date of Instrument" : "effective_date_of_instrument",
            "64_Exercise type" : "exercise_type",
            "67_CIC code of the underlying asset" : "cic_code_of_the_underlying_asset",
            "68_Identification code of the underlying asset" : "identification_code_of_the_underlying_asset",
            "69_Type of identification code for the underlying asset" : "type_of_identification_code_for_the_underlying_asset",
            "70_Name of the underlying asset" : "name_of_the_underlying_asset",
            "71_Quotation currency of the underlying asset (C)" : "quotation_currency_of_the_underlying_asset_c",
            "72_Last valuation price of the underlying asset" : "last_valuation_price_of_the_underlying_asset",
            "73_Country of quotation of the underlying asset" : "country_of_quotation_of_the_underlying_asset",
            "74_Economic Area of quotation of the underlying asset" : "economic_area_of_quotation_of_the_underlying_asset",
            "75_Coupon rate of the underlying asset" : "coupon_rate_of_the_underlying_asset",
            "76_Coupon payment frequency of the underlying asset" :"coupon_payment_frequency_of_the_underlying_asset",
            "77_Maturity date of the underlying asset" : "maturity_date_of_the_underlying_asset",
            "78_Redemption profile of the underlying asset" : "redemption_profile_of_the_underlying_asset",
            "79_Redemption rate of the underlying asset" : "redemption_rate_of_the_underlying_asset",
            "80_Issuer name of the underlying asset" : "issuer_name_of_the_underlying_asset",
            "81_Issuer identification code of the underlying asset" : "issuer_identification_code_of_the_underlying_asset",
            "82_Type of issuer identification code of the underlying asset" : "type_of_issuer_identification_code_of_the_underlying_asset",
            "83_Name of the group of the issuer of the underlying asset" : "name_of_the_group_of_the_issuer_of_the_underlying_asset",
            "84_Identification of the group of the underlying asset" : "identification_of_the_group_of_the_underlying_asset",
            "85_Type of the group identification code of the underlying asset" : "type_of_the_group_identification_code_of_the_underlying_asset",
            "86_Issuer country of the underlying asset" : "issuer_country_of_the_underlying_asset",
            "87_Issuer economic area of the underlying asset" : "issuer_economic_area_of_the_underlying_asset",
            "88_Explicit guarantee by the country of issue of the underlying asset" : "explicit_guarantee_by_the_country_of_issue_of_the_underlying_asset",
            "89_Credit quality step of the underlying asset" : "credit_quality_step_of_the_underlying_asset"}

        codes = "', '".join(self.get_instruments().index.tolist())
        infos = ', '.join(instruments_infos_dict.values())
        query = f"SELECT {infos} FROM intranet.dbo.instrument i"\
                +" INNER JOIN iso_code iso ON iso.id = i.id_iso_code"\
                +f" WHERE identification_code in ('{codes}')"

        db_instruments_infos = pd.read_sql_query(query,
                                                 self.connector)
        
        db_instruments_infos.rename(columns=dict((v, k) for k, v in instruments_infos_dict.items()), inplace=True)                                        
        
        db_instruments_infos.set_index("14_Identification code of the financial instrument", inplace=True)
        bloomberg_infos.set_index("ISIN", inplace=True)

        bloomberg_infos = bloomberg_infos[~bloomberg_infos.index.duplicated()]
        db_instruments_infos = pd.concat([db_instruments_infos, bloomberg_infos], axis=1)

        if self.client == "BIL":
            CASH = self.get_instruments().index[
                self.get_instruments()["asset_type_3"] == "CASH"].astype('str').to_list()
            FET = self.get_instruments().index[
                self.get_instruments()["asset_type_3"] == "F.E.T."].astype('str').to_list()
            OTHER = []
            OTHER += self.get_instruments().index[
                self.get_instruments()["asset_type_3"] == "ACCRUED EXP."].astype('str').to_list()
            OTHER += self.get_instruments().index[
                self.get_instruments()["asset_type_3"] == "RECEIVABLES"].astype('str').to_list()
            OTHER += self.get_instruments().index[
                self.get_instruments()["asset_type_3"] == "PAYABLES"].astype('str').to_list()
            ALL_SP = CASH + FET + OTHER

        sp_instruments_infos = pd.DataFrame(index=ALL_SP, columns=instruments_infos_dict.keys())

        if self.client == "BIL":
            sp_instruments_infos.loc[CASH, 
                "12_CIC code of the instrument"] = "XT72"
            sp_instruments_infos.loc[FET, 
                "12_CIC code of the instrument"] = "XLE2"
            sp_instruments_infos.loc[OTHER, 
                "12_CIC code of the instrument"] = "XT79"
            sp_instruments_infos.loc[:, 
                "13_Economic zone of the quotation place"] = 0
            sp_instruments_infos.loc[:, 
                "15_Type of identification code for the instrument"] = 99
            sp_instruments_infos.loc[:, 
                "21_Quotation currency (A)"].update(
                    self.get_instruments().loc[ALL_SP, "asset_currency"])
            sp_instruments_infos.loc[FET, 
                "65_Hedging Rolling"] = "Y"
            sp_instruments_infos.loc[FET, 
                "70_Name of the underlying asset"] = \
                    self.get_instruments().loc[FET].apply(lambda x: self.write_underlying_name(x), axis=1)
            sp_instruments_infos.loc[FET, 
                "71_Quotation currency of the underlying asset (C)"] = \
                    self.get_instruments().loc[FET, "asset_currency"]

        sp_instruments_infos.loc[:,
            "46_Issuer name"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "46_Issuer name"].iloc[0]
        sp_instruments_infos.loc[:,
            "47_Issuer identification code"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "47_Issuer identification code"].iloc[0]
        sp_instruments_infos.loc[:,
            "49_Name of the group of the issuer"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "49_Name of the group of the issuer"].iloc[0]
        sp_instruments_infos.loc[:,
            "50_Identification of the group"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "50_Identification of the group"].iloc[0]
        sp_instruments_infos.loc[:,
            "51_Type of identification code for issuer group"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "51_Type of identification code for issuer group"].iloc[0]
        sp_instruments_infos.loc[:,
            "52_Issuer country"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "52_Issuer country"].iloc[0]
        sp_instruments_infos.loc[:,
            "53_Issuer economic area"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "53_Issuer economic area"].iloc[0]
        sp_instruments_infos.loc[:,
            "54_Economic sector"] = self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "54_Economic sector"].iloc[0]
              
        self.instruments_infos = sp_instruments_infos.append(db_instruments_infos)
        
        self.instruments_infos.loc[:,
            "16_Grouping code for multiple leg instruments"
            ] = pd.to_numeric(self.instruments["contract_number"])
        self.instruments_infos.loc[:,
            "17_Instrument name"
            ] = self.instruments["asset_name"].astype('str')
        self.instruments_infos.loc[:,
            "39_Maturity date"
            ] = self.instruments["maturity_date"].astype('str')

        self.instruments_infos["59_Credit quality step"] = \
            self.instruments_infos["12_CIC code of the instrument"].apply(
                lambda x:self.map_CQS(x))
        self.instruments_infos["61_Strike price"].where(~self.instruments["contract_number"].notnull(),
                                                        self.get_instruments()["contract_number"].apply(lambda x:self.compute_strike_price(x)),
                                                        inplace=True)

        self.instruments_infos.replace({"Subscription tax IEH": "Subscription tax"}, regex=True, inplace=True)
        self.instruments_infos.replace({"Subscription tax I": "Subscription tax"}, regex=True, inplace=True)
        self.instruments_infos.reset_index(inplace=True)
        self.instruments_infos.drop(columns="14_Identification code of the financial instrument",
                                    inplace=True)
        self.instruments_infos.rename(columns={"index" : "14_Identification code of the financial instrument"}, 
                                      inplace=True)

        self.instruments_infos["21_Quotation currency (A)"].replace({"GBp":"GBP"}, inplace=True)        
        self.instruments_infos["55_Covered / not covered"].replace({"n/a" : np.nan}, regex=True, inplace=True)
        self.instruments_infos["93_Sensitivity to underlying asset price (delta)"].replace("-", np.nan, inplace=True)
        self.instruments_infos["90_Modified Duration to maturity date"].replace("-", np.nan, inplace=True)
        self.instruments_infos["91_Modified duration to next option exercise date"].replace("-", np.nan, inplace=True)
        self.instruments_infos["92_Credit sensitivity"].replace("-", np.nan, inplace=True)
        self.instruments_infos["93_Sensitivity to underlying asset price (delta)"].replace("-", np.nan, inplace=True)
        self.instruments_infos["94_Convexity / gamma for derivatives"].replace("-", np.nan, inplace=True)
        self.instruments_infos["94b_Vega"].replace("-", np.nan, inplace=True)
        
    def get_instruments_infos(self):
        if self.instruments_infos is None:
            self.fetch_instruments_infos()
        return self.instruments_infos

    def compute_strike_price(self, contract_number):
        if contract_number is None:
            return None
        else:
            p_val = self.instruments.loc[(self.get_instruments()["contract_number"] == contract_number) &
                                         (self.get_instruments()["quantity_nominal"] > 0)]["quantity_nominal"].iloc[0]
            n_val = self.instruments.loc[(self.get_instruments()["contract_number"] == contract_number) &
                                         (self.get_instruments()["quantity_nominal"] < 0)]["quantity_nominal"].iloc[0]
            return -(p_val / n_val)

    def map_CQS(self, CIC):
        #"B", 
        if CIC[2] in ["1", "2", "5", "6", "8", "C", "D", "E", "F"] or \
            CIC[2:] in ["73", "74", "75"]:
            return self.AODB_CASH.loc[self.AODB_CASH["Client"] == self.client, "59_Credit quality step"].iloc[0]
        else:
            return np.nan

    def write_underlying_name(self, instrument):
        contract = instrument.name.split('_')[0]
        leg = instrument.name.split('_')[1]
        main = self.get_instruments().loc[contract+'_1',
                                          "asset_currency"]
        underlying = self.get_instruments().loc[contract+'_2',
                                                "asset_currency"]

        if instrument["quantity_nominal"] > 0:
            if leg == '1':
                return main + underlying + ' SPOT Exchange rate'
            else:
                return  underlying + main + ' SPOT Exchange rate'
        else:
            if leg == '2':
                return main + underlying + ' SPOT Exchange rate'
            else:
                return  underlying + main + ' SPOT Exchange rate'