import pyodbc
from pathlib import Path
import pandas as pd
import numpy as np
from timeit import default_timer as timer

from .constants import DB_INSTRUMENTS_INFOS_MAP

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
                 source_dir=None):
        self.connector = pyodbc.connect('driver={SQL Server};'
                                        'Server=DESKTOP-RGN6M86;'
                                        'Database=intranet;'
                                        'Trusted_Connection=yes;')

        self.source_dir = source_dir
        self.bloomberg_infos = None
        self.portfolio_NAVs= None
        self.ccy = None

    def fetch_shareclass_infos(self, isin):

        query = ('SELECT '
                 'id, '
                 'code_isin, '
                 'shareclass, '
                 'shareclass_id, '
                 'shareclass_currency, '
                 'shareclass_name, '
                 'id_subfund, '
                 'type_tpt '
                 'FROM intranet.dbo.shareclass '
                 f"WHERE code_isin='{isin}'")

        return pd.read_sql_query(query, self.connector)

    def fetch_isins_in_group(self, indicator, id_subfund):
        isins = pd.read_sql_query('SELECT '
                                 'code_isin '
                                 'FROM intranet.dbo.shareclass '
                                 f"WHERE id_subfund='{id_subfund}'"
                                 f"AND (shareclass_id='{indicator}' "
                                 f"OR shareclass='{indicator}') "
                                 "AND supprime=0",
                                 self.connector)

        return isins["code_isin"].tolist()
                                 
    def fetch_subfund_infos(self, subfund_id):
        #'fund_issuer_code,'
        #'fund_issuer_code_type, '
        self.subfund_infos = pd.read_sql_query('SELECT '
                                               'id, '
                                               'subfund_name, '
                                               'subfund_code, '
                                               'subfund_cic, '
                                               'subfund_nace, '
                                               'subfund_lei, '
                                               'subfund_currency, '
                                               'id_fund '
                                               'FROM intranet.dbo.subfund '
                                               f"WHERE id='{subfund_id}'", 
                                               self.connector)

        return self.subfund_infos

    def fetch_fund_infos(self, fund_id):
        #'fund_issuer_group_code, '
        #'fund_issuer_group_code_type, '
        #'fund_issuer_group_name, '
        #'fund_issuer_country, '
        #'fund_custodian_country, '
        #'custodian_name '
        fund_infos = pd.read_sql_query('SELECT '
                                       'fund_name, '
                                       'fund_issuer_group_code, '
                                       'fund_country, '
                                       'depositary_name, '
                                       'depositary_lei, '
                                       'depositary_group_name, '
                                       'depositary_group_lei, '
                                       'depositary_country, '
                                       'depositary_nace '
                                       'FROM intranet.dbo.fund as f '
                                       'INNER JOIN intranet.dbo.depositary as d '
                                       'ON f.id_depositary=d.id '
                                       f"WHERE f.id='{fund_id}'", 
                                       self.connector)
        fund_infos.rename(columns={"depositary_lei":"fund_issuer_code"}, inplace=True)
        fund_infos["issuer_economic_area"] = pd.read_sql_query('SELECT '
                                                               'geographic '
                                                               'FROM intranet.dbo.iso_code '
                                                               f"WHERE iso_code_2='{fund_infos['fund_country'].iloc[0]}'",
                                                               self.connector)
        return fund_infos

    def fetch_shareclass_nav(self,
                             sc_id,
                             sc_curr,
                             sf_curr,
                             date):

        nav = pd.read_sql_query('SELECT '
                                'nav_date, '
                                'share_price, '
                                'outstanding_shares, '
                                'shareclass_total_net_asset, '
                                'subfund_total_net_asset '
                                'FROM intranet.dbo.nav '
                                f"WHERE id_shareclass='{sc_id}' "
                                f"AND nav_date='{date}' "
                                f"AND nav_currency='{sc_curr}'",
                                self.connector)
        
        nav.rename(columns={"shareclass_total_net_asset":
                            "shareclass_total_net_asset_sc_curr"},
                   inplace=True)

        nav['shareclass_total_net_asset_sf_curr'] = pd.read_sql_query('SELECT '
                                                                      'shareclass_total_net_asset '
                                                                      'FROM intranet.dbo.nav '
                                                                      f"WHERE id_shareclass='{sc_id}' "
                                                                      f"AND nav_date='{date}' "
                                                                      f"AND nav_currency='{sf_curr}'",
                                                                      self.connector)    
        return nav

    def fetch_subfund_shareclasses(self, id_subfund):
        isins = pd.read_sql_query('SELECT '
                                  'code_isin '
                                  'FROM intranet.dbo.shareclass '
                                  f"WHERE id_subfund='{id_subfund}'"
                                  "AND supprime=0",
                                  self.connector)

        return isins["code_isin"].tolist()

    def fetch_instruments(self, subfund_id, date, client):
        infos = ', '.join(["asset_id_string",
                           "hedge_indicator",
                           "asset_name",
                           "asset_type",
                           "asset_type_3",
                           "asset_currency",
                           "quantity_nominal",
                           "price_market",
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
                +f"AND portfolio_date='{date}'"

        instruments = pd.read_sql_query(query,
                                        self.connector)

        instruments.rename(columns={"asset_id_string":"14_Identification code of the financial instrument"}, inplace=True)
        instruments.set_index("14_Identification code of the financial instrument", inplace=True)

        if instruments.index.duplicated().any():
            fused = pd.DataFrame(columns=["quantity_nominal"
                                          "market_and_accrued_fund",
                                          "market_fund",
                                          "accrued_fund",
                                          "market_and_accrued_asset",
                                          "market_asset",
                                          "accrued_asset"])
            fused["quantity_nominal"] = instruments.groupby("14_Identification code of the financial instrument")["quantity_nominal"].sum()
            fused["market_and_accrued_fund"] = instruments.groupby("14_Identification code of the financial instrument")["market_and_accrued_fund"].sum()
            fused["market_fund"] = instruments.groupby("14_Identification code of the financial instrument")["market_fund"].sum()
            fused["accrued_fund"] = instruments.groupby("14_Identification code of the financial instrument")["accrued_fund"].sum()
            fused["market_and_accrued_asset"] = instruments.groupby("14_Identification code of the financial instrument")["market_and_accrued_asset"].sum()
            fused["market_asset"] = instruments.groupby("14_Identification code of the financial instrument")["market_asset"].sum()
            fused["accrued_asset"] = instruments.groupby("14_Identification code of the financial instrument")["accrued_asset"].sum()

            instruments = instruments[~instruments.index.duplicated()]
            instruments["quantity_nominal"].update(fused["quantity_nominal"])
            instruments["market_and_accrued_fund"].update(fused["market_and_accrued_fund"])
            instruments["market_fund"].update(fused["market_fund"])
            instruments["accrued_fund"].update(fused["accrued_fund"])
            instruments["market_and_accrued_asset"].update(fused["market_and_accrued_asset"])
            instruments["market_asset"].update(fused["market_asset"])
            instruments["accrued_asset"].update(fused["accrued_asset"])

        instruments["accrued_fund"].fillna(0, inplace=True)
        instruments["accrued_asset"].fillna(0, inplace=True)
        
        if client == "BIL":
            instruments["market_fund"] = instruments["market_and_accrued_fund"] - instruments["accrued_fund"]
            instruments["market_asset"] = instruments["market_and_accrued_asset"] - instruments["accrued_asset"]
        elif client == "Dynasty":
            instruments["market_and_accrued_asset"] = instruments["market_asset"] + instruments["accrued_asset"]
        
        return instruments

    def fetch_db_instruments_infos(self, id_list):

        #######################################################################
        ##################### GET MISSING DATA FROM EXCEL #####################
        #######################################################################
        self.fetch_missing_infos(id_list)
        #######################################################################

        codes = "', '".join(id_list)
        infos = ', '.join(DB_INSTRUMENTS_INFOS_MAP.keys())
        query = f"SELECT {infos} FROM intranet.dbo.instrument i"\
                +" INNER JOIN iso_code iso ON iso.id = i.id_iso_code"\
                +f" WHERE identification_code in ('{codes}')"

        db_instruments_infos = pd.read_sql_query(query,
                                                 self.connector)
        db_instruments_infos.rename(columns=DB_INSTRUMENTS_INFOS_MAP, inplace=True)                                        
        
        db_instruments_infos.set_index("14_Identification code of the financial instrument", inplace=True)
        self.bloomberg_infos.set_index("ISIN", inplace=True)

        self.bloomberg_infos = self.bloomberg_infos[~self.bloomberg_infos.index.duplicated()]
        db_instruments_infos = pd.concat([db_instruments_infos, self.bloomberg_infos], axis=1)

        return db_instruments_infos
  
    def fetch_missing_infos(self, id_list):
        AODB_file_name = "AO Data Base v0.8.xlsx"
        BBG_file_name = "AO_Bloomberg_Template_SII.xlsx"
        AODB_file_path = self.source_dir / AODB_file_name
        BBG_file_path = self.source_dir / BBG_file_name
        
        if not (self.source_dir / "AODB_CASH.feather").exists():
            self.AODB_CASH = pd.read_excel(AODB_file_path, sheet_name="Cash", skiprows=1)
            self.AODB_CASH.to_feather(self.source_dir / "AODB_CASH.feather")
        else:
            self.AODB_CASH = pd.read_feather(self.source_dir / "AODB_CASH.feather")

        if not (self.source_dir / "BBG.feather").exists():
            BBG = pd.read_excel(BBG_file_path, sheet_name="Hard Copy", skiprows=2)
            BBG.replace({"-": np.nan}, inplace=True)
            BBG["YAS_RISK"] = pd.to_numeric(BBG["YAS_RISK"])
            BBG.to_feather(self.source_dir / "BBG.feather")
        else:
            BBG = pd.read_feather(self.source_dir / "BBG.feather")

        if not (self.source_dir / "currency.feather").exists():
            ccy = pd.read_excel(BBG_file_path, sheet_name="ccy", usecols="E,F", names=["pair", "rate"])
            self.ccy = ccy.set_index("pair")["rate"]
            ccy_save = self.ccy.reset_index()
            ccy_save.to_feather(self.source_dir / "currency.feather")
        else:
            self.ccy = pd.read_feather(self.source_dir / "currency.feather").set_index("pair", inplace=True)

        self.bloomberg_infos = BBG.loc[BBG["ISIN"].isin(id_list),
                                       ["ISIN",
                                        "YAS_RISK",
                                        "YAS_MOD_DUR",
                                        "CV_MODEL_DELTA_S",
                                        "DUR_ADJ_MTY_MID",
                                        "cv_model_gamma_v",
                                        "CV_MODEL_vega",
                                        "cv_model_cnvs_prem",
                                        "Bond floor"]]
        
        self.bloomberg_infos.rename(
            columns={"YAS_RISK" : "92_Credit sensitivity",
                     "YAS_MOD_DUR" : "91_Modified duration to next option exercise date",
                     "CV_MODEL_DELTA_S" : "93_Sensitivity to underlying asset price (delta)",
                     "DUR_ADJ_MTY_MID" : "90_Modified Duration to maturity date",
                     "cv_model_gamma_v" : "94_Convexity / gamma for derivatives",
                     "CV_MODEL_vega" : "94b_Vega",
                     "cv_model_cnvs_prem" : "128_Option premium (convertible instrument only)",
                     "Bond floor" : "127_Bond Floor (convertible instrument only)"},
            inplace=True)
