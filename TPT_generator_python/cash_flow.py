import pandas as pd
from timeit import default_timer as timer

class CashFlow():
    def __init__(self, data_bucket):
        self.data_bucket = data_bucket
        self.date = data_bucket.date
        self.actualized = False
        self.cash_flows_dict = {}
        self.load_rates(data_bucket.source_dir)

    def compute(self):
        data = self.data_bucket.get_instruments_infos(
                   info=["12_CIC code of the instrument",
                         "21_Quotation currency (A)",
                         "33_Coupon rate",
                         "38_Coupon payment frequency",
                         "39_Maturity date",
                         "40_Redemption type",
                         "43_Call / put date",
                         "45_Strike price for embedded (call/put) options"])

        data["quantity_nominal"] = self.data_bucket.get_instruments("quantity_nominal")
        data["7_Reporting date"] = pd.to_datetime(self.date)
        data["43_Call / put date"] = pd.to_datetime(data["43_Call / put date"])
        data["39_Maturity date"] = pd.to_datetime(data["39_Maturity date"], errors='coerce')

        self.cash_flows_dict = {instrument[0] : self.compute_single(instrument[1]) for instrument in data.iterrows()}
        self.actualized = True

    def compute_single(self, instrument):
        frequency = instrument["38_Coupon payment frequency"]
        coupon = instrument["33_Coupon rate"]
        notional = instrument["quantity_nominal"]
        cic = instrument["12_CIC code of the instrument"]
        strike = instrument["45_Strike price for embedded (call/put) options"]
        currency = instrument["21_Quotation currency (A)"]
        repdate = instrument["7_Reporting date"]
        optdate = instrument["43_Call / put date"]
        matdate = instrument["39_Maturity date"]
        redempt_type = instrument["40_Redemption type"]

        
        if cic[2] in ["1", "2", "5"] and redempt_type != "Defaulted":
            if pd.isnull(matdate):
                if pd.isnull(optdate):
                    optdate = repdate + pd.offsets.DateOffset(years=40)
                matdate = pd.to_datetime(optdate)

            if frequency == 0:
                if pd.isnull(optdate) or optdate < repdate:
                    if optdate < repdate:
                        print(["[WARNING] optdate < repdate"])
                    dates = [matdate]
                else:
                    dates = [optdate]
            else:
                offset = pd.DateOffset(months=-12/frequency)
                d = matdate + offset
                dates = [matdate]
                while d > repdate:
                    dates = [d] + dates
                    d = dates[0] + offset
                if not pd.isnull(optdate):
                    dates.append(optdate)
                    
            dates = pd.DatetimeIndex(dates)
            if not pd.isnull(optdate):
                dates = dates[dates <= optdate]
            if dates.has_duplicates:
                dates.drop_duplicates()

            CF = pd.DataFrame(index=dates, columns=["rfr", "up", "down"])
            if frequency == 0:
                if not pd.isnull(optdate) and matdate != optdate:
                    CF["rfr"] = coupon / 100 * notional + notional * strike / 100
                    CF["up"] = coupon / 100 * notional + notional * strike / 100
                    CF["down"] = coupon / 100 * notional + notional * strike / 100
                else:
                    CF["rfr"] = coupon / 100 * notional + notional
                    CF["up"] = coupon / 100 * notional + notional
                    CF["down"] = coupon / 100 * notional + notional
            else:
                CF["rfr"] = (coupon / frequency) / 100 * notional
                CF["up"] = (coupon / frequency) / 100 * notional
                CF["down"] = (coupon / frequency) / 100 * notional
                if not pd.isnull(optdate):
                    if matdate != optdate:
                        d2 = matdate
                        d1 = d2 + offset
                        while d1 > optdate:
                            d2 = d1
                            d1 = d2 + offset
                        CF.loc[optdate, "rfr"] = (coupon / frequency) \
                            / 100 * notional * ((optdate - d1) / (d2 - d1)) \
                            + notional * strike / 100
                        CF.loc[optdate, "up"] = (coupon / frequency) \
                            / 100 * notional * ((optdate - d1) / (d2 - d1)) \
                            + notional * strike / 100
                        CF.loc[optdate, "down"] = (coupon / frequency) \
                            / 100 * notional * ((optdate - d1) / (d2 - d1)) \
                            + notional * strike / 100
                    else:
                        CF.loc[optdate, "rfr"] = (coupon / frequency) \
                            / 100 * notional + notional * strike / 100
                        CF.loc[optdate, "up"] = (coupon / frequency) \
                            / 100 * notional + notional * strike / 100
                        CF.loc[optdate, "down"] = (coupon / frequency) \
                            / 100 * notional + notional * strike / 100
                else:
                    CF.loc[matdate, "rfr"] = (coupon / frequency) \
                        / 100 * notional + notional
                    CF.loc[matdate, "up"] = (coupon / frequency) \
                        / 100 * notional + notional
                    CF.loc[matdate, "down"] = (coupon / frequency) \
                        / 100 * notional + notional
            if '2020-11-21' in CF.index:
                breakpoint()
            CF["rfr"] = CF["rfr"] * self.RFR.loc[CF.index, currency]
            CF["up"] = CF["up"] * self.UP.loc[CF.index, currency]
            CF["down"] = CF["down"] * self.DOWN.loc[CF.index, currency]
        else:
            CF = pd.DataFrame(index=["not bond"], columns=["rfr", "up", "down"])
            CF["rfr"] = 0
            CF["up"] = 0
            CF["down"] = 0
        
        return CF

    def load_rates(self, root_path):
        CF_file_name = f"Template Cash flows TPT_{self.date}.xlsm"
        RFR_feather = f"RFR_{self.date}.feather"
        UP_feather = f"UP_{self.date}.feather"
        DOWN_feather = f"DOWN_{self.date}.feather"
        cols = "A,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U"
        col_names = ["dates", "EUR", "DKK", "HUF", "NOK", "PLN", "RUB", "SEK",
                     "CHF", "GBP", "AUD", "CAD", "HKD", "INR", "JPY", "MYR",
                     "SGD", "KRW", "TWD", "USD"]

        if not (root_path / RFR_feather).exists():
            self.RFR = pd.read_excel(root_path / CF_file_name,
                                     sheet_name="rfr",
                                     skiprows=9,
                                     index_col=0,
                                     names=col_names,
                                     usecols=cols)
            RFR_save = self.RFR.reset_index()
            RFR_save.to_feather(root_path / RFR_feather)
        else:
            self.RFR = pd.read_feather(root_path / RFR_feather)
            self.RFR.set_index("dates", inplace=True)

        if not (root_path / UP_feather).exists():
            self.UP = pd.read_excel(root_path / CF_file_name,
                                    sheet_name="up",
                                    skiprows=9,
                                    index_col=0,
                                    names=col_names,
                                    usecols=cols)
            UP_save = self.UP.reset_index()
            UP_save.to_feather(root_path / UP_feather)
        else:
            self.UP = pd.read_feather(root_path / UP_feather)
            self.UP.set_index("dates", inplace=True)
        if not (root_path / DOWN_feather).exists():
            self.DOWN = pd.read_excel(root_path / CF_file_name,
                                      sheet_name="down",
                                      skiprows=9,
                                      index_col=0,
                                      names=col_names,
                                      usecols=cols)
            DOWN_save = self.DOWN.reset_index()
            DOWN_save.to_feather(root_path / DOWN_feather)
        else:
            self.DOWN = pd.read_feather(root_path / DOWN_feather)
            self.DOWN.set_index("dates", inplace=True)

    def __getitem__(self, key):
        return self.cash_flows_dict[key]