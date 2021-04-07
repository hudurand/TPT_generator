import pandas as pd
import numpy as np
import logging
from .constants import DB_INSTRUMENTS_INFOS_MAP, FIELDS, ALL, RATINGS
from pandas.testing import assert_index_equal


class DataProcessor():

    def __init__(self, data_bucket):
        self.data_bucket = data_bucket
        self.fetcher = data_bucket.fetcher

        self.logger = logging.getLogger(__name__)
        self.logger.info("Processor initialised")

    def process_instruments(self):
        self.logger.info("processing instruments")
        self.logger.info(f"instruments shape: {self.data_bucket.instruments.shape}")
        self.logger.debug(f"""
        instruments:
{self.data_bucket.instruments} 
        """)

        self.data_bucket.instruments.rename(columns={"asset_id_string":"instrument"}, inplace=True)
        self.data_bucket.instruments["instrument"] = self.data_bucket.instruments["instrument"].astype('str')
        self.data_bucket.instruments.set_index("instrument", inplace=True)

        if self.data_bucket.instruments.index.duplicated().any():
            fused = pd.DataFrame(columns=["quantity_nominal",
                                          "market_value_fund",
                                          "clear_value_fund",
                                          "accrued_fund",
                                          "market_value_asset",
                                          "clear_value_asset",
                                          "accrued_asset"])
            fused["quantity_nominal"] = self.data_bucket.instruments.groupby("instrument")["quantity_nominal"].sum()
            fused["market_value_fund"] = self.data_bucket.instruments.groupby("instrument")["market_value_fund"].sum()
            fused["clear_value_fund"] = self.data_bucket.instruments.groupby("instrument")["clear_value_fund"].sum()
            fused["accrued_fund"] = self.data_bucket.instruments.groupby("instrument")["accrued_fund"].sum()
            fused["market_value_asset"] = self.data_bucket.instruments.groupby("instrument")["market_value_asset"].sum()
            fused["clear_value_asset"] = self.data_bucket.instruments.groupby("instrument")["clear_value_asset"].sum()
            fused["accrued_asset"] = self.data_bucket.instruments.groupby("instrument")["accrued_asset"].sum()

            self.data_bucket.instruments = self.data_bucket.instruments[~self.data_bucket.instruments.index.duplicated()]
            self.data_bucket.instruments["quantity_nominal"].update(fused["quantity_nominal"])
            self.data_bucket.instruments["market_value_fund"].update(fused["market_value_fund"])
            self.data_bucket.instruments["clear_value_fund"].update(fused["clear_value_fund"])
            self.data_bucket.instruments["accrued_fund"].update(fused["accrued_fund"])
            self.data_bucket.instruments["market_value_asset"].update(fused["market_value_asset"])
            self.data_bucket.instruments["clear_value_asset"].update(fused["clear_value_asset"])
            self.data_bucket.instruments["accrued_asset"].update(fused["accrued_asset"])

        self.data_bucket.instruments["accrued_fund"].fillna(0, inplace=True)
        self.data_bucket.instruments["accrued_asset"].fillna(0, inplace=True)
        
        if self.data_bucket.client == "BIL":
            self.data_bucket.instruments["clear_value_fund"] = self.data_bucket.instruments["market_value_fund"] - self.data_bucket.instruments["accrued_fund"]
            self.data_bucket.instruments["clear_value_asset"] = self.data_bucket.instruments["market_value_asset"] - self.data_bucket.instruments["accrued_asset"]
        elif self.data_bucket.client == "Dynasty":
            self.data_bucket.instruments["market_value_asset"] = self.data_bucket.instruments["clear_value_asset"] + self.data_bucket.instruments["accrued_asset"]
        
        self.logger.info("processed instruments")
        self.logger.info(f"instruments shape: {self.data_bucket.instruments.shape}")
        self.logger.debug(f"""
        instruments:
{self.data_bucket.instruments} 
        """)

    def clean_instruments_infos(self):
        self.logger.info("cleaning instruments infos")
        self.logger.info(f"instruments_infos shape: {self.data_bucket.instruments_infos.shape}")

        self.data_bucket.instruments_infos.loc[:,
            "16_Grouping code for multiple leg instruments"
            ] = self.data_bucket.get_instruments("grouping_id")
        self.data_bucket.instruments_infos.loc[:,
            "17_Instrument name"
            ] = self.data_bucket.get_instruments("asset_name").astype('str')
        self.data_bucket.instruments_infos.loc[:,
            "39_Maturity date"
            ] = self.data_bucket.get_instruments("maturity_date").astype('str')

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

    def process_instruments_infos(self):
        self.logger.info("processing instruments infos")

        if self.data_bucket.instruments_infos[FIELDS["59"]].isnull().values.any():
            self.compute_59()
        if FIELDS["137"] not in self.data_bucket.instruments_infos.columns:
            self.compute_137()
        self.compute_strike_prices()

    def compute_processing_data(self):
        self.logger.info("compute processing data")

        if self.data_bucket.processing_data is None:
            self.data_bucket.init_processing_data()

        self.compute_distribution_matrix()
        self.compute_qn_det()
        self.compute_131()
        self.compute_market_exposure()

    def compute_distribution_matrix(self):
        self.logger.info("compute distribution matrix")

        instruments = self.data_bucket.get_instruments_by_index(
            ALL, 
            info=["id_group", "market_value_fund"])

        shareclasses = self.data_bucket.get_subfund_shareclasses()
        navs = pd.DataFrame(index=shareclasses, 
                            columns=["shareclass_total_net_asset_sf_ccy",
                                     "subfund_total_net_asset",
                                     "groups"],
                            dtype=object)

        #navs["groups"] = navs["groups"].astype(object)
        for isin in shareclasses:
            navs.loc[isin, "shareclass_total_net_asset_sf_ccy"] = \
                self.data_bucket.get_shareclass_nav(isin=isin, info="shareclass_total_net_asset_sf_ccy")
            navs.loc[isin, "subfund_total_net_asset"] = \
                self.data_bucket.get_shareclass_nav(isin=isin, info="subfund_total_net_asset")
            navs.at[isin, "groups"] = self.data_bucket.get_groups(isin).tolist()

        betas = pd.DataFrame(1, index=instruments.index, columns=navs.index)
        for isin in shareclasses:
            betas[isin].where(
                instruments["id_group"].isin(navs.loc[isin, "groups"]),
                0,
                inplace=True)
        betas.sort_index(inplace=True)
        betas["fund"] = 1
        for isin in shareclasses:
            betas["fund"] = betas["fund"] * betas[isin]

        shareclass_keys = pd.DataFrame(0, index=instruments.index, columns=navs.index)
        for isin in shareclasses:
            shareclass_keys[isin].where(
                ~(instruments["id_group"].isin(navs.loc[isin, "groups"])),
                navs.loc[isin, "shareclass_total_net_asset_sf_ccy"].astype('float64'),
                inplace=True)
        shareclass_keys.sort_index(inplace=True)

        distributed = pd.DataFrame(0, index=instruments.index, columns=navs.index)
        for isin in shareclasses:
            distributed.loc[betas["fund"]==0, isin] = \
                instruments.loc[betas["fund"]==0, "market_value_fund"] \
                * shareclass_keys.loc[betas["fund"]==0, isin] / shareclass_keys.loc[betas["fund"]==0].sum(axis=1)

        for isin in shareclasses:
            distributed.loc[betas["fund"]==1, isin] = \
                instruments.loc[betas["fund"]==1, "market_value_fund"] \
                * (shareclass_keys.loc[betas["fund"]==1, isin] \
                   - distributed.loc[((betas[isin]==1) & (betas["fund"]==0)), isin].sum()) \
                / (shareclass_keys.loc[betas["fund"]==1].sum(axis=1) \
                   - instruments.loc[betas["fund"]==0, "market_value_fund"].sum())

        distributed.index.names = ["instrument"]
        for isin in shareclasses:
            index0 = self.data_bucket.processing_data.loc[(ALL, isin), ALL].index.get_level_values(0)
            assert_index_equal(index0, distributed.index)
            self.data_bucket.processing_data.loc[(ALL, isin), "distribution"] = distributed[isin].values
            self.data_bucket.processing_data.loc[(ALL, isin), "valuation weight"] = \
                distributed[isin].values / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_ccy")
            self.data_bucket.processing_data.loc[(ALL, isin), "distribution weight"] = \
                self.data_bucket.processing_data.loc[(ALL, isin), "distribution"] \
                / self.data_bucket.processing_data.loc[(ALL, isin), "market_value_fund"]
            
            # take the opportunity to compute total cash value
            self.data_bucket.shareclass_infos.loc[isin, "cash"] = \
                self.data_bucket.processing_data.xs(isin, level="shareclass").loc[
                    self.data_bucket.processing_data.xs(isin, level="shareclass")[FIELDS["12"]] == "XT72"]["distribution"].sum() \
                * self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sc_ccy") \
                / self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_ccy")

    def compute_qn_det(self):
        
        price = self.data_bucket.get_instruments_by_index(ALL, info="market_price")
        amount = self.data_bucket.get_instruments_by_index(ALL, info="quantity_nominal")
        value = self.data_bucket.get_instruments_by_index(ALL, info="clear_value_asset")

        self.data_bucket.instruments["QN"] = price * amount / value

        self.data_bucket.processing_data = self.data_bucket.processing_data.join(self.data_bucket.instruments["QN"])

    def set_sp_instrument_infos(self):
        self.logger.info("setting specific instruments infos")
        
        columns = DB_INSTRUMENTS_INFOS_MAP.values()
        instruments = self.data_bucket.get_instruments_by_index(ALL)
        cash_index, fet_index, other_index = self.loc_sp_instruments()
        sp_index = cash_index + fet_index + other_index

        sp_instruments_infos = pd.DataFrame(index=sp_index,
                                            columns=columns)
        sp_instruments_infos.loc[cash_index, 
            "12_CIC code of the instrument"] = "XT72"

        if len(fet_index) != 0:
            sp_instruments_infos.loc[fet_index, 
                "12_CIC code of the instrument"] = "XLE2"
            sp_instruments_infos.loc[fet_index, 
                "65_Hedging Rolling"] = "Y"
            sp_instruments_infos.loc[fet_index, 
                "70_Name of the underlying asset"] = instruments.loc[
                    fet_index].apply(lambda x: 
                        self.write_underlying_name(x), axis=1)
            sp_instruments_infos.loc[fet_index, 
                "71_Quotation currency of the underlying asset (C)"] = \
                    instruments.loc[fet_index, "asset_currency"]
        else:
            sp_instruments_infos["65_Hedging Rolling"] = ""

        sp_instruments_infos.loc[other_index, 
            "12_CIC code of the instrument"] = "XT79"
        sp_instruments_infos.loc[:, 
            "13_Economic zone of the quotation place"] = 0
        sp_instruments_infos.loc[:, 
            "15_Type of identification code for the instrument"] = 99
        sp_instruments_infos.loc[:, 
            "21_Quotation currency (A)"].update(
                instruments.loc[sp_index, "asset_currency"])
        
        sp_instruments_infos.loc[:, "46_Issuer name"] = self.data_bucket.get_fund_infos("depositary_name")
        sp_instruments_infos.loc[:, "47_Issuer identification code"] = self.data_bucket.get_fund_infos("fund_issuer_code")
        sp_instruments_infos.loc[:, "49_Name of the group of the issuer"] = self.data_bucket.get_fund_infos("depositary_group_name")
        sp_instruments_infos.loc[:, "50_Identification of the group"] = self.data_bucket.get_fund_infos("depositary_group_lei")
        sp_instruments_infos.loc[:, "52_Issuer country"] = self.data_bucket.get_fund_infos("depositary_country")
        sp_instruments_infos.loc[:, "53_Issuer economic area"] = self.data_bucket.get_fund_infos("issuer_economic_area")
        sp_instruments_infos.loc[:, "54_Economic sector"] = self.data_bucket.get_fund_infos("depositary_nace")
        
        self.logger.info(f"specific instruments infos shape: {sp_instruments_infos.shape}")
        self.logger.debug(f"""
        cash instruments infos: {len(cash_index)}
{sp_instruments_infos.loc[cash_index, ['12_CIC code of the instrument',
                                       '54_Economic sector',
                                       '70_Name of the underlying asset']]} 
        """)
        self.logger.debug(f"""
        fet instruments infos: {len(fet_index)}
{sp_instruments_infos.loc[fet_index, ['12_CIC code of the instrument',
                                       '54_Economic sector',
                                       '70_Name of the underlying asset']]} 
        """)
        self.logger.debug(f"""
        other instruments infos: {len(other_index)}
{sp_instruments_infos.loc[other_index, ['12_CIC code of the instrument',
                                       '54_Economic sector',
                                       '70_Name of the underlying asset']]} 
        """)

        return sp_instruments_infos


    def loc_sp_instruments(self):
        instruments = self.data_bucket.get_instruments_by_index(ALL)

        CASH = instruments.index[
            instruments["asset_type"] == "CASH"].astype('str').to_list()

        FET = instruments.index[
            instruments["asset_type"] == "CAT"].astype('str').to_list()

        OTHER = instruments.index[
            instruments["asset_type"] == "TRES"].astype('str').to_list()

        return CASH, FET, OTHER
  
    def compute_strike_prices(self):
        forward_strike_price = self.data_bucket.get_instruments_by_index(
            ALL,
            info="grouping_id",
            ).apply(lambda x: self.compute_strike_price_single(x))
        self.data_bucket.instruments_infos["61_Strike price"].where(forward_strike_price.isnull(),
                                                                    forward_strike_price,
                                                                    inplace=True) 

    def compute_strike_price_single(self, grouping_id):
        instruments = self.data_bucket.get_instruments_by_index(ALL)
        if grouping_id is None:
            return None
        else:
            p_val = instruments.loc[(instruments["grouping_id"] == grouping_id) &
                                         (instruments["quantity_nominal"] > 0)]["quantity_nominal"].iloc[0]
            n_val = instruments.loc[(instruments["grouping_id"] == grouping_id) &
                                         (instruments["quantity_nominal"] < 0)]["quantity_nominal"].iloc[0]
            return -(p_val / n_val)

    def write_underlying_name(self, row):
        instruments = self.data_bucket.instruments

        contract = row.name.split('_')[0]
        leg = row.name.split('_')[1]
        main = instruments.loc[contract+'_sell',
                               "asset_currency"]
        underlying = instruments.loc[contract+'_buy',
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
            #     get one before lowest rating and map to CQS
            # default rating based on NACE
            # if required and no infos 9
            if (row[FIELDS["12"]][2] in ["1", "2", "5", "6", "8", "C", "D", "E", "F"]
                or row[FIELDS["12"]][2:] in ["73", "74", "75"]):
                    #if (row[FIELDS["54"]] == "K6419"
                    #    or row[FIELDS["54"]][0] in ["D", "O"]):
                    if row[FIELDS["54"]] == "K6419":
                        return 3
                    else:
                        return 9
                #return 9
            else:
                return np.nan
        
        self.data_bucket.instruments_infos[FIELDS["59"]] = \
            self.data_bucket.instruments_infos.apply(lambda row: select_value(row), axis=1)

        def map_ratings(x, y):
            if y is not None: 
                return RATINGS[x.name][y]
        def sort_and_select(x):
            x.sort()
            i = min(len(x), 2)-1
            return x[i]

        ratings = self.data_bucket.get_instruments(['rating_moodys','rating_sp','rating_fitch'])
        ratings = ratings.loc[~(ratings.isnull().all(axis=1))]
        #breakpoint()
        ratings.replace({"u": "",
                 "p": "",
                 " \*\+": "",
                 " \*\-": "",
                 "e": "",
                 "(P)": "",
                 "(EXP)": "",
                 "-e": "",
                 "\+\+": "+",
                 " \*": ""}, regex=True, inplace=True)
        ratings.replace(["WD", "NA", "NR"], None, regex=True, inplace=True)
        ratings = ratings.apply(lambda x: x.apply(lambda y: map_ratings(x,y)))
        CQS = ratings.apply(lambda x: x.dropna().tolist(), axis=1)
        CQS = CQS.apply(lambda x: sort_and_select(x))

        self.data_bucket.instruments_infos[FIELDS["59"]].update(CQS)


    def compute_131(self):
        def select_value(row):
            if row[FIELDS["12"]][2] == "3":
                if row[FIELDS["12"]][:2] == "XL":
                    return "3X"
                else:
                    return "3L"
            if row[FIELDS["12"]][2] in ["7", "8", "0"] and\
               row["distribution"] < 0:
                return "L"
            
            return row[FIELDS["12"]][2]

        self.data_bucket.processing_data[FIELDS["131"]] = \
            self.data_bucket.processing_data.apply(
                lambda row: select_value(row), axis=1)
    
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
        
        try:
            self.data_bucket.instruments_infos[FIELDS["137"]] = \
                self.data_bucket.instruments_infos.apply(lambda row: select_value(row), axis=1)
        except:
            breakpoint()

    def compute_market_exposure(self):
        pattern = "..22|..A2|..B4"
        self.data_bucket.processing_data["ME"] = \
            self.data_bucket.processing_data["distribution"].where(
                ~self.data_bucket.processing_data[FIELDS["12"]].str.match(pattern),
                self.data_bucket.processing_data.apply(
                    lambda x: self.compute_mrktexp_exception(x), axis=1))
    
    def compute_mrktexp_exception(self, row):
        CIC = row[FIELDS["12"]]
        V = row["distribution"]

        CC = row[FIELDS["72"]] if \
             not pd.isnull(row[FIELDS["72"]]) else 1
        
        W = row[FIELDS["20"]]
        BS = row["61_Strike price"] if \
             not pd.isnull(row["61_Strike price"]) else 1

        if not pd.isnull(row[FIELDS["71"]]):
            main_ccy = row[FIELDS["21"]]
            underlying_ccy = row[FIELDS["71"]]
        
            # GBp is pence: must divide by 100 the price of underlying
            if main_ccy != underlying_ccy:
                EX = self.fetcher.ccy.loc[main_ccy + underlying_ccy, "rate"]
            else: 
                EX = 1
        
        else: 
            EX = 1
    
        if CIC[2:] == "22":
            CX = row[FIELDS["93"]] \
                if not pd.isnull(row[FIELDS["93"]]) else 1
            ME = max(V / BS * CC * EX * CX, 
                     row["distribution"])
        
        elif CIC[2:] == "A2":
            ME = min(V * W/100 * CC * EX, 
                     row["distribution"])
            
        elif CIC[2:] == "B4":
            BT = row[FIELDS["62"]] \
                if not pd.isnull(row[FIELDS["62"]]) else 1
            ME = max(V * BT * (CC-BS) * EX,
                     row["distribution"])

        else:
            ME = row["distribution"]
        return ME