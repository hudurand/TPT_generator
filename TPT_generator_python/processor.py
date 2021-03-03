import pandas as pd
import numpy as np

def loc_sp_instruments(client, instruments):
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

def set_sp_instrument_infos(sp_instruments_infos,
                            instruments,
                            cash_index,
                            fet_index,
                            other_index,
                            AODB_CASH,
                            client):

        sp_index = cash_index + fet_index + other_index
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
                    write_underlying_name(instruments, x), axis=1)
        sp_instruments_infos.loc[fet_index, 
            "71_Quotation currency of the underlying asset (C)"] = \
                instruments.loc[fet_index, "asset_currency"]
        
        sp_instruments_infos.loc[:, "46_Issuer name"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "46_Issuer name"].iloc[0]
        sp_instruments_infos.loc[:, "47_Issuer identification code"] = \
                AODB_CASH.loc[AODB_CASH["Client"] == client, "47_Issuer identification code"].iloc[0]
        sp_instruments_infos.loc[:, "49_Name of the group of the issuer"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "49_Name of the group of the issuer"].iloc[0]
        sp_instruments_infos.loc[:, "50_Identification of the group"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "50_Identification of the group"].iloc[0]
        sp_instruments_infos.loc[:, "51_Type of identification code for issuer group"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "51_Type of identification code for issuer group"].iloc[0]
        sp_instruments_infos.loc[:, "52_Issuer country"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "52_Issuer country"].iloc[0]
        sp_instruments_infos.loc[:, "53_Issuer economic area"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "53_Issuer economic area"].iloc[0]
        sp_instruments_infos.loc[:, "54_Economic sector"] = \
            AODB_CASH.loc[AODB_CASH["Client"] == client, "54_Economic sector"].iloc[0]  
        
def compute_strike_price(instruments, contract_number):
    if contract_number is None:
        return None
    else:
        p_val = instruments.loc[(instruments["contract_number"] == contract_number) &
                                     (instruments["quantity_nominal"] > 0)]["quantity_nominal"].iloc[0]
        n_val = instruments.loc[(instruments["contract_number"] == contract_number) &
                                     (instruments["quantity_nominal"] < 0)]["quantity_nominal"].iloc[0]
        return -(p_val / n_val)

def map_CQS(CIC, client, AODB):
    #TODO: this is very wrong
    # use field 54 -> code NACE
    # based in EU
    if CIC[2] in ["1", "2", "5", "6", "8", "C", "D", "E", "F"] or \
        CIC[2:] in ["73", "74", "75"]:
        # TODO: this should be in the client table
        return AODB.loc[AODB["Client"] == client, "59_Credit quality step"].iloc[0]
    else:
        return np.nan

def write_underlying_name(instruments, row):
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

class Data_Processor():

    def __init__(self,
                 data_bucket,
                 fetcher):
        
        self.fetcher = fetcher
        self.data_bucket = data_bucket

    #TODO: rename
    #TODO: replace by matrix computation
    def compute_sp_distribution(self, isin, dedicated_group):
        # sum the value of cash instruments in shareclass
        #TODO: rename
        dedicated_value = self.data_bucket.get_instruments(
            indicator=[dedicated_group], info="market_and_accrued_fund").sum()

        isins = self.data_bucket.get_isins_in_group(dedicated_group)

        NAV = self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr",
                                      isin=isin)
        TOTAL_NAV = 0
        
        for i in isins:
            TOTAL_NAV += self.data_bucket.get_shareclass_nav(info="shareclass_total_net_asset_sf_curr",
                                                 isin=i)
        #print("included_cash", included_cash)
        #print("NAV ", NAV)
        #print("TOTAL NAV ", TOTAL_NAV)
        return  TOTAL_NAV, NAV * (1 - dedicated_value / TOTAL_NAV)
        #NAV - included_cash * (NAV / TOTAL_NAV)
    
    def compute_QN(self):
        price = self.data_bucket.get_instruments("price_market")
        amount = self.data_bucket.get_instruments("quantity_nominal")
        value = self.data_bucket.get_instruments("market_asset")

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

        self.data_bucket.instruments_infos["59_Credit quality step"] = \
            self.data_bucket.instruments_infos["12_CIC code of the instrument"].apply(
                lambda x: map_CQS(x, self.fetcher.client, self.fetcher.AODB_CASH))
        self.data_bucket.instruments_infos["61_Strike price"].where(~self.data_bucket.get_instruments("contract_number").notnull(),
                                                        self.data_bucket.get_instruments("contract_number").apply(lambda x: compute_strike_price(self.data_bucket.get_instruments(), x)),
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
        
    def get_distribution_matrix(self):
        instruments = bucket.get_instruments(indicator="all", info=["hedge_indicator",
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