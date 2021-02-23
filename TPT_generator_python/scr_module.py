import pandas as pd
import numpy as np

class SCR_Module():
    def __init__(self):
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
                6 : (0.0, 4.5)},
            1 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (5.5, 0.6),
                3 : (7.0, 0.7),
                4 : (12.5, 1.5),
                5 : (22.5, 2.5),
                6 : (22.5, 2.5)},
            2 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (8.4, 0.5),
                3 : (10.5, 0.5),
                4 : (20.0, 1.0),
                5 : (35.0, 1.8),
                6 : (35.0, 1.8)},
            3 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (10.9, 0.5),
                3 : (13.0, 0.5),
                4 : (25.0, 1.0),
                5 : (44.0, 0.5),
                6 : (44.0, 0.5)},
            4 : {
                0 : (0.0, 0.0),
                1 : (0.0, 0.0),
                2 : (13.4, 0.5),
                3 : (15.5, 0.5),
                4 : (30.0, 0.5),
                5 : (46.5, 0.5),
                6 : (46.5, 0.5)}
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


    def compute_total_SCR_market_risk(self):
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

    def spread_risk_parameter(self, bond_type, duration, CQS):
        duration_group = min(duration // 5, 4)
        if bond_type == 1:
            a, b = self.general_bonds_param[duration_group][9]            
        elif bond_type == 2:
            a, b = self.covered_bonds_param[duration_group][CQS]
        elif bond_type == 3:
            a, b = self.government_bonds_params[duration_group][CQS]

        #print("bond type: ", bond_type)
        #print("durations: ", duration, duration_group)
        #print("CQS: ", CQS)
        #print(a, b)
        return min(a/100 + (b/100) * (duration - duration_group * 5), 1)

    def currency_risk_parameter(self, curr1, curr2):
        currencies = ["EUR", "DKK", "BGN", "XOF", "XAF", "KMF"]

        if curr1 in currencies and curr2 in currencies:
            return self.curr_param[currencies.index(curr1), currencies.index(curr2)]/100
        else:
            return 0.25