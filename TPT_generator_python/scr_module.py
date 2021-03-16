import pandas as pd
import numpy as np

from .cash_flow import CashFlow
from .constants import FIELDS

class SCRModule():
    def __init__(self, data_bucket):
        self.data_bucket = data_bucket
        self.cash_flows = CashFlow(self.data_bucket)
        self.define_parameters()
        self.sym_adj = -0.48

    def compute_total_scr_market_risk(self):
        #submodules_list = ["IR", "EQ", "PR", "SP", "CO", "FX"]

        return "total SCR market risk"

    def compute_interest_rate_risk_submodule(self):
        # SCR_IR_UP = TPT_report[fields["97"].sum()
        # SCR_IR_DOWN = TPT_report[fields["98"].sum()
        # max(SCR_IR_UP, SCR_IR_DOWN]])
        return "interest rate market risk"

    def compute_equity_risk_submodule(self):
        # fields["99"] = fields["26"] * (0.39 + sym_adj)
        # fields["100"] = fields["30"] * (0.49 + sym_adj)
        # /!\ derogatory assets /!\
        # SCR_eq1 = TPT_report[fields["99"]].sum()
        # SCR_eq2 = TPT_report[fields["100"]].sum()
        # SCR_eq = sqrt(SCR_eq1**2 + 2*0.75*SCR_eq1*(SCR_eq2 + SCR_quinf + SCR_quinfc) + (SCR_eq2 + SCR_quinf + SCR_quinfc)**2)
        return "equity risk market risk"

    def compute_property_risk_submodule(self):
        # SCR_prop = SCR_bonds + SCR_sec + SCR_cd
        return "property risk submodule"

    def compute_spread_risk_submodule(self):
        return "spreak risk submodule"

    def compute_currency_risk_submodule(self):
        return "currency risk submodule"

    def compute_market_risk_concentrations_submodule(self):
        return "market risk concentration submodule"

    def compute_scr(self):
        self.compute_97()
        self.compute_98()
        self.compute_99()
        self.compute_100()
        self.compute_102()
        self.compute_105a()
        self.compute_105b()

    def spread_risk_parameter(self, bond_type, duration, cqs):
        duration_group = min(duration // 5, 4)
        if bond_type == 1:
            a, b = self.general_bonds_param[duration_group][cqs]            
        elif bond_type == 2:
            a, b = self.covered_bonds_param[duration_group][cqs]
        elif bond_type == 3:
            a, b = self.government_bonds_params[duration_group][cqs]

        return min(a/100 + (b/100) * (duration - duration_group * 5), 1)

    def currency_risk_parameter(self, curr1, curr2):
        currencies = ["EUR", "DKK", "BGN", "XOF", "XAF", "KMF"]

        if curr1 in currencies and curr2 in currencies:
            return self.curr_param[currencies.index(curr1), currencies.index(curr2)]/100
        else:
            return 0.25
    
    def compute_97(self):
        if not self.cash_flows.actualized:
            self.cash_flows.compute()
        
        self.data_bucket.scr[FIELDS["97"]] = \
            self.data_bucket.get_processing_data([FIELDS["12"],
                                                 "valuation weight"]).apply(
                lambda row: self.compute_97_single(row), axis=1)

    def compute_97_single(self, row):
        if row[FIELDS["12"]][2] in ["1", "2", "5"]:
            if self.cash_flows[row.name]["rfr"].sum() == 0:
                return 0
            else:
                return (1 - self.cash_flows[row.name]["up"].sum() \
                            / self.cash_flows[row.name]["rfr"].sum()) \
                            * row["valuation weight"]
        return 0
    
    def compute_98(self):
        if not self.cash_flows.actualized:
            self.cash_flows.compute()

        self.data_bucket.scr[FIELDS["98"]] = \
            self.data_bucket.get_processing_data([FIELDS["12"],
                                                 "valuation weight"]).apply(
                lambda row: self.compute_98_single(row), axis=1)

    
    def compute_98_single(self, row):
        if row[FIELDS["12"]][2] in ["1", "2", "5"]:
            if self.cash_flows[row.name]["rfr"].sum() == 0:
                return 0
            else:
                return (1 - self.cash_flows[row.name]["down"].sum() \
                            / self.cash_flows[row.name]["rfr"].sum()) \
                            * row["valuation weight"]
        return 0

    def compute_99(self):
        self.data_bucket.scr[FIELDS["99"]] = \
            self.data_bucket.get_processing_data([FIELDS["12"],
                                                  FIELDS["131"],
                                                 "valuation weight"]).apply(
                lambda row: self.shock_down_type1(row), axis=1)
#        self.data_bucket.scr[FIELDS["99"]] = \
#            self.data_bucket.get_instruments_infos().apply(
#                lambda row: self.shock_down_type1(row), axis=1)

    def shock_down_type1(self, row):
        if row[FIELDS["131"]] == "3L":
            return row["valuation weight"] * (0.39 + self.sym_adj/100)

        elif row[FIELDS["12"]][2:] == "22":
            if not pd.isnull(row[FIELDS["71"]]):
                main_ccy = row[FIELDS["21"]]
                underlying_ccy = row[FIELDS["71"]]
            
                if main_ccy != underlying_ccy:
                    EX = self.data_bucket.fetcher.ccy[main_ccy + underlying_ccy]
                else: 
                    EX = 1

            else: 
                EX = 1
                
            E = row[FIELDS["5"]]
            T = row[FIELDS["18"]]
            U = row[FIELDS["19"]]
            AA = row[FIELDS["23"]]
            AE = row[FIELDS["25"]]
            BS = row[FIELDS["61"]]
            CC = row[FIELDS["72"]]
            CX = row[FIELDS["93"]]

            return ((T + U) / BS * CC * EX * (CX / E) * (AE / AA)) * (0.39 + self.sym_adj/100)

        else:
            return 0

    def compute_100(self):
        self.data_bucket.scr[FIELDS["100"]] = \
            self.data_bucket.get_processing_data([FIELDS["12"],
                                                  FIELDS["131"],
                                                  "ME"]).apply(
                lambda row: self.shock_down_type2(row), axis=1)

    def shock_down_type2(self, row):
        if row[FIELDS["131"]] in ["4", "3X"] or\
            row[FIELDS["12"]][2:] in ["B1", "B4"]:
            return row["ME"] * (0.49 + self.sym_adj/100)
        else:
            return 0

    def compute_102(self):
        self.data_bucket.scr[FIELDS["102"]] = \
            self.data_bucket.get_processing_data("valuation weight") \
            * self.data_bucket.get_processing_data([FIELDS["12"],
                                                    FIELDS["39"],
                                                    FIELDS["55"],
                                                    FIELDS["53"],
                                                    FIELDS["59"],
                                                    FIELDS["90"],
                                                    FIELDS["131"],
                                                    "valuation weight"]).apply(
                lambda row: self.shock_down_spread(row), axis=1)

    def shock_down_spread(self, row):
        #print("\n", row.name)
        if row[FIELDS["131"]] in ["B", "E"]\
            or row[FIELDS["39"]] == "nan":
            return 0

        # Government bonds
        elif row[FIELDS["131"]] == "1":
            if row[FIELDS["53"]] == "1":
                return 0
            else:
                duration = row[FIELDS["90"]]
                CQS = row[FIELDS["59"]]
                shock = self.spread_risk_parameter(3, duration, CQS)

        # Covered bonds
        elif row[FIELDS["55"]] == "C"\
            and row[FIELDS["59"]] < 2:
            duration = row[FIELDS["90"]]
            CQS = row[FIELDS["59"]]
            shock = self.spread_risk_parameter(2, duration, CQS)

        # General bonds and loans
        elif row[FIELDS["12"]][2] in ["2", "8"]:
            duration = row[FIELDS["90"]]
            CQS = row[FIELDS["59"]]
            shock = self.spread_risk_parameter(1, duration, CQS)
        else:
            return 0 

        return shock

    def compute_105a(self):
        self.data_bucket.scr[FIELDS["105a"]] = \
            self.data_bucket.get_processing_data([FIELDS["21"],
                                                  "ME",
                                                  "valuation weight"]).apply(
                lambda row: self.shock_up_currency(row), axis=1)

    def shock_up_currency(self, row):
        fund_curr = self.data_bucket.get_shareclass_infos("shareclass_currency")
        quot_curr = row[FIELDS["21"]] 
        if quot_curr != fund_curr :
            return row["ME"] * self.currency_risk_parameter(fund_curr, quot_curr)
        else:
            return 0

    def compute_105b(self):
        self.data_bucket.scr[FIELDS["105b"]] = \
            self.data_bucket.get_processing_data([FIELDS["21"],
                                                  "ME",
                                                  "valuation weight"]).apply(
                lambda row: self.shock_down_currency(row), axis=1)

    def shock_down_currency(self, row):
        fund_curr = self.data_bucket.get_shareclass_infos("shareclass_currency")
        quot_curr = row[FIELDS["21"]] 
        if quot_curr != fund_curr :
            return - row["ME"] * self.currency_risk_parameter(fund_curr, quot_curr)
        else:
            return 0
    
    def define_parameters(self):
        self.general_bonds_param = {
            0 : {
                0 : (0.0, 0.9),
                1 : (0.0, 1.1),
                2 : (0.0, 1.4),
                3 : (0.0, 2.5),
                4 : (0.0, 4.5),
                5 : (0.0, 7.5),
                6 : (0.0, 7.5),
                9 : (0.0, 3.0)},
            1 : {
                0 : (4.5, 0.5),
                1 : (5.5, 0.6),
                2 : (7.0, 0.7),
                3 : (12.5, 1.5),
                4 : (22.5, 2.5),
                5 : (37.5, 4.2),
                6 : (37.5, 4.2),
                9 : (15.0, 1.7)},
            2 : {
                0 : (7.0, 0.5),
                1 : (8.5, 0.5),
                2 : (10.5, 0.5),
                3 : (20.0, 1.0),
                4 : (35.0, 1.8),
                5 : (58.5, 0.5),
                6 : (58.5, 0.5),
                9 : (23.5, 1.2)},
            3 : {
                0 : (9.5, 0.5),
                1 : (11.0, 0.5),
                2 : (13.0, 0.5),
                3 : (25.0, 1.0),
                4 : (44.0, 0.5),
                5 : (61.0, 0.5),
                6 : (61.0, 0.5),
                9 : (23.5, 1.2)},
            4 : {
                0 : (12.0, 0.5),
                1 : (13.5, 0.5),
                2 : (15.5, 0.5),
                3 : (30.0, 0.5),
                4 : (46.6, 0.5),
                5 : (63.5, 0.5),
                6 : (63.5, 0.5),
                9 : (35.5, 0.5)}
        }

        self.covered_bonds_param = {
            0 : {
                0 : (0.0, 0.7),
                1 : (0.0, 0.9)},
            1 : {
                0 : (3.5, 0.5),
                1 : (4.5, 0.5)}
        }

        self.government_bonds_params = {
            0 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (0.0, 1.1),
                3 : (0.0, 1.4),
                4 : (0.0, 2.5),
                5 : (0.0, 4.5),
                6 : (0.0, 4.5),
                9 : (0.0, 3.0)},
            1 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (5.5, 0.6),
                3 : (7.0, 0.7),
                4 : (12.5, 1.5),
                5 : (22.5, 2.5),
                6 : (22.5, 2.5),
                9 : (15.0, 1.7)},
            2 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (8.4, 0.5),
                3 : (10.5, 0.5),
                4 : (20.0, 1.0),
                5 : (35.0, 1.8),
                6 : (35.0, 1.8),
                9 : (23.5, 1.2)},
            3 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (10.9, 0.5),
                3 : (13.0, 0.5),
                4 : (25.0, 1.0),
                5 : (44.0, 0.5),
                6 : (44.0, 0.5),
                9 : (23.5, 1.2)},
            4 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (13.4, 0.5),
                3 : (15.5, 0.5),
                4 : (30.0, 0.5),
                5 : (46.5, 0.5),
                6 : (46.5, 0.5),
                9 : (35.5, 0.5)}
        }

        self.unrated = {
            0 : (0 , 3)
        }
        
        self.curr_param = np.array([[0.00, 0.39, 1.81, 2.18, 1.96, 2.00],
                                [0.39, 0.00, 2.24, 2.62, 2.40, 2.44],
                                [1.81, 2.24, 0.00, 4.06, 3.85, 3.89],
                                [2.18, 2.62, 4.06, 0.00, 4.23, 4.27],
                                [1.96, 2.40, 3.85, 4.23, 0.00, 4.04],
                                [2.00, 2.44, 3.89, 4.27, 4.04, 0.00]])
        A = 0.5
        self.correlation_matrix = np.array([[   1,    A,    A,    A,    0, 0.25],
                                            [   A,    1, 0.75, 0.75,    0, 0.25],
                                            [   A, 0.75,    1,  0.5,    0, 0.25],
                                            [   A, 0.75,  0.5,    1,    0, 0.25],
                                            [   0,    0,    0,    0,    1,    0],
                                            [0.25, 0.25, 0.25, 0.25,    0,    1]])
