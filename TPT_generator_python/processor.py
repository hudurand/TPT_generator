import pandas as pd
import numpy as np

def get_sp_instrument_infos(client, instruments, columns):
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
            ALL_SP = CASH + FET + OTHER

            sp_instruments_infos = pd.DataFrame(index=ALL_SP, columns=columns)

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
                    instruments.loc[ALL_SP, "asset_currency"])
            sp_instruments_infos.loc[FET, 
                "65_Hedging Rolling"] = "Y"
            sp_instruments_infos.loc[FET, 
                "70_Name of the underlying asset"] = \
                    instruments.loc[FET].apply(lambda x: write_underlying_name(instruments, x), axis=1)
            sp_instruments_infos.loc[FET, 
                "71_Quotation currency of the underlying asset (C)"] = \
                    instruments.loc[FET, "asset_currency"]

        return sp_instruments_infos

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
    #"B", 
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

def compute_hedge_sc(hedge_indicator, NAV, instruments):
    hedged_value = instruments.loc[instruments["hedge_indicator"]==hedge_indicator, "quantity_nominal"].sum()

    # sum value of instruments associated only with this shareclass
    # substract the result from the nav of the shareclass
    return NAV - hedged_value   