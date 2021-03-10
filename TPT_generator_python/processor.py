import pandas as pd
import numpy as np
from .constants import DB_INSTRUMENTS_INFOS_MAP, FIELDS

class Data_Processor():

    def __init__(self, data_bucket):
        self.data_bucket = data_bucket
        self.fetcher = data_bucket.fetcher

    def process_instruments(self):
        if "QN" not in self.data_bucket.instruments_infos.columns:
            self.data_bucket.instruments_infos["QN"] = np.nan
            self.compute_QN()
        
        if "ME" not in self.data_bucket.instruments_infos.columns:
            self.data_bucket.instruments_infos["ME"] = np.nan
            self.compute_market_exposure()

        if FIELDS["131"] not in self.data_bucket.instruments_infos.columns:
            self.compute_131()

        if self.data_bucket.instruments_infos[FIELDS["59"]].isnull().values.all():
            self.compute_59()

    def compute_market_exposure(self):
        pattern = "..22|..A2|..B4"

        self.data_bucket.instruments_infos["ME"] = \
            self.data_bucket.get_instruments("market_and_accrued_asset").where(
                ~self.data_bucket.get_instruments_infos(info=FIELDS["12"]).str.match(pattern),
                self.data_bucket.get_instruments_infos().apply(
                    lambda x: self.compute_ME_exception(x), axis=1))
        
    def compute_QN(self):
        price = self.data_bucket.get_instruments(info="price_market", indicator="all")
        amount = self.data_bucket.get_instruments(info="quantity_nominal", indicator="all")
        value = self.data_bucket.get_instruments(info="market_asset", indicator="all")

        self.data_bucket.instruments["QN"] = price * amount / value 

    def clean_instruments_infos(self):
        self.data_bucket.instruments_infos.loc[:,
            "16_Grouping code for multiple leg instruments"
            ] = pd.to_numeric(self.data_bucket.get_instruments("contract_number"))
        self.data_bucket.instruments_infos.loc[:,
            "17_Instrument name"
            ] = self.data_bucket.get_instruments("asset_name").astype('str')
        self.data_bucket.instruments_infos.loc[:,
            "39_Maturity date"
            ] = self.data_bucket.get_instruments("maturity_date").astype('str')

        #self.data_bucket.instruments_infos["59_Credit quality step"] = \
        #    self.data_bucket.instruments_infos["12_CIC code of the instrument"].apply(
        #        lambda x: self.map_CQS(x))
        self.data_bucket.instruments_infos["61_Strike price"].where(~self.data_bucket.get_instruments("contract_number").notnull(),
                                                        self.data_bucket.get_instruments("contract_number").apply(lambda x: self.compute_strike_price(x)),
                                                        inplace=True)

        self.data_bucket.instruments_infos["17_Instrument name"].replace({"Subscription tax IEH": "Subscription tax"}, regex=True, inplace=True)
        self.data_bucket.instruments_infos["17_Instrument name"].replace({"Subscription tax I": "Subscription tax"}, regex=True, inplace=True)
        self.data_bucket.instruments_infos["21_Quotation currency (A)"].replace({"GBp":"GBP"}, inplace=True)        
        self.data_bucket.instruments_infos["55_Covered / not covered"].replace({"n/a" : np.nan}, regex=True, inplace=True)
        self.data_bucket.instruments_infos["93_Sensitivity to underlying asset price (delta)"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["90_Modified Duration to maturity date"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["91_Modified duration to next option exercise date"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["92_Credit sensitivity"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["93_Sensitivity to underlying asset price (delta)"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["94_Convexity / gamma for derivatives"].replace("-", np.nan, inplace=True)
        self.data_bucket.instruments_infos["94b_Vega"].replace("-", np.nan, inplace=True)
        
    def compute_distribution_matrix(self):
        instruments = self.data_bucket.get_instruments(indicator="all", info=["hedge_indicator",
                                                                              "market_and_accrued_fund"])
        shareclasses = self.data_bucket.get_subfund_shareclasses()
        NAVs = pd.DataFrame(index=shareclasses, 
                    columns=["shareclass_total_net_asset_sf_curr",
                             "subfund_total_net_asset",
                             "indicators"],
                    dtype=object)

        NAVs["indicators"] = NAVs["indicators"].astype(object)
        for isin in shareclasses:
            NAVs.loc[isin, "shareclass_total_net_asset_sf_curr"] = \
                self.data_bucket.get_shareclass_nav(isin=isin, info="shareclass_total_net_asset_sf_curr")
            NAVs.loc[isin, "subfund_total_net_asset"] = \
                self.data_bucket.get_shareclass_nav(isin=isin, info="subfund_total_net_asset")
            NAVs.at[isin, "indicators"] = \
                [self.data_bucket.get_subfund_infos("subfund_indicator"),
                 self.data_bucket.get_shareclass_infos(isin=isin, info="shareclass"),
                 self.data_bucket.get_shareclass_infos(isin=isin, info="shareclass_id")]

        BETAS = pd.DataFrame(1, index=instruments.index, columns=NAVs.index)
        for isin in shareclasses:
            BETAS[isin].where(
                instruments["hedge_indicator"].isin(NAVs.loc[isin, "indicators"]),
                0,
                inplace=True)
        BETAS.sort_index(inplace=True)
        BETAS["fund"] = 1
        for isin in shareclasses:
            BETAS["fund"] = BETAS["fund"] * BETAS[isin]

        SK = pd.DataFrame(0, index=instruments.index, columns=NAVs.index)
        for isin in shareclasses:
            SK[isin].where(
                ~(instruments["hedge_indicator"].isin(NAVs.loc[isin, "indicators"])),
                NAVs.loc[isin, "shareclass_total_net_asset_sf_curr"].astype('float64'),
                inplace=True)
        SK.sort_index(inplace=True)

        D = pd.DataFrame(0, index=instruments.index, columns=NAVs.index)
        for isin in shareclasses:
            D.loc[BETAS["fund"]==0, isin] = \
                instruments.loc[BETAS["fund"]==0, "market_and_accrued_fund"] \
                * SK.loc[BETAS["fund"]==0, isin] / SK.loc[BETAS["fund"]==0].sum(axis=1)

        for isin in shareclasses:
            D.loc[BETAS["fund"]==1, isin] = \
                instruments.loc[BETAS["fund"]==1, "market_and_accrued_fund"] \
                * (SK.loc[BETAS["fund"]==1, isin] \
                   - D.loc[((BETAS[isin]==1) & (BETAS["fund"]==0)), isin].sum()) \
                / (SK.loc[BETAS["fund"]==1].sum(axis=1) \
                   - instruments.loc[BETAS["fund"]==0, "market_and_accrued_fund"].sum())

        return D

    def set_sp_instrument_infos(self):
        columns = DB_INSTRUMENTS_INFOS_MAP.values()
        instruments = self.data_bucket.get_instruments(indicator="all")
        cash_index, fet_index, other_index = self.loc_sp_instruments()
        sp_index = cash_index + fet_index + other_index

        sp_instruments_infos = pd.DataFrame(index=sp_index,
                                            columns=columns)
        sp_instruments_infos.loc[cash_index, 
            "12_CIC code of the instrument"] = "XT72"
        sp_instruments_infos.loc[fet_index, 
            "12_CIC code of the instrument"] = "XLE2"
        sp_instruments_infos.loc[other_index, 
            "12_CIC code of the instrument"] = "XT79"
        sp_instruments_infos.loc[:, 
            "13_Economic zone of the quotation place"] = 0
        sp_instruments_infos.loc[:, 
            "15_Type of identification code for the instrument"] = 99
        sp_instruments_infos.loc[:, 
            "21_Quotation currency (A)"].update(
                instruments.loc[sp_index, "asset_currency"])
        sp_instruments_infos.loc[fet_index, 
            "65_Hedging Rolling"] = "Y"
        sp_instruments_infos.loc[fet_index, 
            "70_Name of the underlying asset"] = instruments.loc[
                fet_index].apply(lambda x: 
                    self.write_underlying_name(x), axis=1)
        sp_instruments_infos.loc[fet_index, 
            "71_Quotation currency of the underlying asset (C)"] = \
                instruments.loc[fet_index, "asset_currency"]
        
        sp_instruments_infos.loc[:, "46_Issuer name"] = self.data_bucket.get_fund_infos("depositary_name")
        sp_instruments_infos.loc[:, "47_Issuer identification code"] = self.data_bucket.get_fund_infos("fund_issuer_code")
        sp_instruments_infos.loc[:, "49_Name of the group of the issuer"] = self.data_bucket.get_fund_infos("depositary_group_name")
        sp_instruments_infos.loc[:, "50_Identification of the group"] = self.data_bucket.get_fund_infos("depositary_group_lei")
        #sp_instruments_infos.loc[:, "51_Type of identification code for issuer group"] = self.data_bucket.get_fund_infos("")
        sp_instruments_infos.loc[:, "52_Issuer country"] = self.data_bucket.get_fund_infos("depositary_country")
        sp_instruments_infos.loc[:, "53_Issuer economic area"] = self.data_bucket.get_fund_infos("issuer_economic_area")
        sp_instruments_infos.loc[:, "54_Economic sector"] = self.data_bucket.get_fund_infos("depositary_nace")
        
        return sp_instruments_infos

    def loc_sp_instruments(self):
        client = self.data_bucket.client
        instruments = self.data_bucket.get_instruments(indicator="all")

        if client == "BIL":
            CASH = instruments.index[
                instruments["asset_type_3"] == "CASH"].astype('str').to_list()

            FET = instruments.index[
                instruments["asset_type_3"] == "F.E.T."].astype('str').to_list()

            OTHER = []
            OTHER += instruments.index[
                instruments["asset_type_3"] == "ACCRUED EXP."].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type_3"] == "RECEIVABLES"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type_3"] == "PAYABLES"].astype('str').to_list()
    
        if client == "Dynasty":
            CASH = instruments.index[
                instruments["asset_type"] == "T010 - Current cash account"].astype('str').to_list()

            FET = instruments.index[
                instruments["asset_type"] == "221 - Forward Exchange Trades (FET)"].astype('str').to_list()

            OTHER = []
            OTHER += instruments.index[
                instruments["asset_type"] == "T035 - Cash Collateral given"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T061 - Amortisation Formation Expenses"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T060 - Fees to be paid"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "902 - Interests Receivable / Payable"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T043 - Cash payable redemptions"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T042 - Cash receivable subscriptions"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T070 - Miscellaneous income"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T040 - Cash receivable sales"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T037 - Transitory collateral account"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T031 - Cash Collateral Received"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "T041 - Cash payable purchases"].astype('str').to_list()
            OTHER += instruments.index[
                instruments["asset_type"] == "901 - Dividends Receivable / Payable"].astype('str').to_list()

        return CASH, FET, OTHER
  
    def compute_strike_price(self, contract_number):
        instruments = self.data_bucket.get_instruments()

        if contract_number is None:
            return None
        else:
            p_val = instruments.loc[(instruments["contract_number"] == contract_number) &
                                         (instruments["quantity_nominal"] > 0)]["quantity_nominal"].iloc[0]
            n_val = instruments.loc[(instruments["contract_number"] == contract_number) &
                                         (instruments["quantity_nominal"] < 0)]["quantity_nominal"].iloc[0]
            return -(p_val / n_val)

    def write_underlying_name(self, row):
        instruments = self.data_bucket.instruments

        contract = row.name.split('_')[0]
        leg = row.name.split('_')[1]
        main = instruments.loc[contract+'_1',
                               "asset_currency"]
        underlying = instruments.loc[contract+'_2',
                                     "asset_currency"]

        if row["quantity_nominal"] > 0:
            if leg == '1':
                return main + underlying + ' SPOT Exchange rate'
            else:
                return  underlying + main + ' SPOT Exchange rate'
        else:
            if leg == '2':
                return main + underlying + ' SPOT Exchange rate'
            else:
                return  underlying + main + ' SPOT Exchange rate'

    def compute_59(self):
        def select_value(row):
            # use field 54 -> code NACE
            # based in EU
            # required only for some CIC code
            # provided by the client: fetch from db
            # default rating based on NACE
            # if required and no infos 9
            if row[FIELDS["12"]][2] in ["1", "2", "5", "6", "8", "C", "D", "E", "F"] or \
                row[FIELDS["12"]][2:] in ["73", "74", "75"]:
                    if row[FIELDS["54"]] == "K6419":
                        return 3
                    else:
                        return 9
            else:
                return np.nan
        
        self.data_bucket.instruments_infos[FIELDS["59"]] = \
            self.data_bucket.instruments_infos.apply(lambda row: select_value(row), axis=1)

    def compute_131(self):
        def select_value(row):
            if row[FIELDS["12"]][2] == "3":
                if row[FIELDS["12"]][:2] == "XL":
                    return "3X"
                else:
                    return "3L"
            if row[FIELDS["12"]][2] in ["7", "8", "0"] and\
               self.data_bucket.get_valuation_weight_vector().loc[row.name] < 0:
                return "L"
            
            return row[FIELDS["12"]][2]

        self.data_bucket.instruments_infos[FIELDS["131"]] = \
            self.data_bucket.instruments_infos.apply(lambda row: select_value(row), axis=1)
    
    def compute_137(self):
        def select_value(row):
            if row[FIELDS["15"]] == 1:
                return np.nan
            if row[FIELDS["12"]][2] == "1":
                return 10
            if row[FIELDS["12"]][2] == "6":
                return 6
            if row[FIELDS["12"]][2] == "7":
                if row[FIELDS["54"]][0] == "K" or\
                   row[FIELDS["54"]][0] == "O":
                    return 12
                else:
                    return 13
            if row[FIELDS["54"]] == "K6411":
                return 1
            if row[FIELDS["54"]] == "K6419":
                return 2
            if row[FIELDS["54"]] == "K6630":
                if row[FIELDS["12"]][2:] == "43":
                    return 3
                else:
                    return 4
            if row[FIELDS["54"]] == "K6619" or\
               row[FIELDS["54"]][:3] == "K649":
                return 5
            if row[FIELDS["54"]][:3] == "K651" or\
               row[FIELDS["54"]][:3] == "K652":
                return 7
            if row[FIELDS["54"]][:3] == "K653":
                return 8
            if row[FIELDS["54"]][0] == "T":
                return 
            if row[FIELDS["54"]][0] not in ["K","T"]:
                return 9
        
        self.data_bucket.instruments_infos[FIELDS["137"]] = \
            self.data_bucket.instruments_infos.apply(lambda row: select_value(row), axis=1)
    
    def compute_ME_exception(self, row):
        CIC = row[FIELDS["12"]]

        V = self.data_bucket.get_instruments("quantity_nominal").loc[row.name] \
            * self.data_bucket.get_distribution_weight().loc[row.name]

        CC = row[FIELDS["72"]] if \
             not pd.isnull(row[FIELDS["72"]]) else 1
        
        W = row[FIELDS["20"]]
        BS = row[FIELDS["61"]]

        if not pd.isnull(row[FIELDS["71"]]):
            main_ccy = row[FIELDS["21"]]
            underlying_ccy = row[FIELDS["71"]]
        
            if main_ccy != underlying_ccy:
                EX = self.fetcher.ccy[main_ccy + underlying_ccy]
            else: 
                EX = 1
        
        else: 
            EX = 1
    
        if CIC[2:] == "22":
            CX = row[FIELDS["93"]] \
                if not pd.isnull(row[FIELDS["93"]]) else 1
            ME = max(V / BS * CC * EX * CX, 
                     self.data_bucket.get_instruments("market_and_accrued_asset").loc[row.name])
        
        elif CIC[2:] == "A2":
            ME = min(V * W/100 * CC * EX, 
                     self.data_bucket.get_instruments("market_and_accrued_asset").loc[row.name])
            
        elif CIC[2:] == "B4":
            BT = row[FIELDS["62"]] \
                if not pd.isnull(row[FIELDS["62"]]) else 1
            ME = max(V * BT * (CC-BS) * EX,
                     self.data_bucket.get_instruments("market_and_accrued_asset").loc[row.name])

        else:
            ME = self.data_bucket.get_instruments("market_and_accrued_asset").loc[row.name]
        return ME