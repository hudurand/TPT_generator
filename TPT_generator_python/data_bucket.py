import pandas as pd
from .processor import loc_sp_instruments
from .processor import Data_Processor
from .constants import DB_INSTRUMENTS_INFOS_MAP

class Data_Bucket():
    """
    Container class to to hold the data coming from the database as well as
    resulting preprocessing in a semi-structured way and feed them to the 
    generator for generation of the report.

    It relies on a fetcher object to get the data from the database and a 
    processor object to apply transformations to the data.
    """

    def __init__(self,
                 client,
                 fetcher):

        self.client = client
        self.fetcher = fetcher
        self.processor = Data_Processor(self,
                                        self.fetcher)
        self.shareclass_infos = None
        self.shareclass_nav = None
        self.subfund_infos = None
        self.fund_infos = None
        self.instruments = None
        self.instruments_infos = None
        self.distribution_matrix = None
    
    def fetch(self):
        """
        Run all data-acquiring methods at once and each of them will in turn
        call their corresponding fetching methods to get data from database.
        """
        self.get_shareclass_infos()
        self.get_shareclass_nav()
        self.get_subfund_infos()
        self.get_fund_infos()
        self.get_instruments()
        self.get_instruments_infos()

    def get_shareclass_infos(self, info=None, isin=None):
        """
        Feed shareclass related informations.
        Args:
            info ([str, list?]): name of the information to return, \
                (name used in database).
            isin (str): isin code of the shareclass, if none the isin stored as\
                attribute of the fetcher is used.
        """

        if isin is None and self.shareclass_infos is None:
            self.shareclass_infos = self.fetcher.fetch_shareclass_infos()

        if isin is None and info is None:
            return self.shareclass_infos
        elif info is None:
            return self.fetcher.fetch_shareclass_infos(isin=isin)
        else:
            return self.fetcher.fetch_shareclass_infos(isin=isin)[info].iloc[0]

    def get_shareclass_nav(self, info=None, isin=None):
        if isin is None:
            sc_id = self.get_shareclass_infos("id")
        else:
            sc_id = self.get_shareclass_infos(isin=isin, info="id")
        #print(isin)
        #print(sc_id)
        sc_curr = self.get_shareclass_infos(isin=isin,
                                            info="shareclass_currency")
        sf_curr = self.get_subfund_infos("subfund_currency")

        if isin is None and self.shareclass_nav is None:
            self.shareclass_nav = self.fetcher.fetch_shareclass_nav(sc_id,
                                                                    sc_curr,
                                                                    sf_curr)
        
        #print("isin: ", isin)
        #print("info: ", info)

        if isin is None and info is None:
            return self.shareclass_nav
        elif info is None:
            return self.fetcher.fetch_shareclass_nav(sc_id,
                                                     sc_curr,
                                                     sf_curr)
        else:
            return self.fetcher.fetch_shareclass_nav(sc_id,
                                                     sc_curr,
                                                     sf_curr)[info].iloc[0]

    def get_subfund_infos(self, info=None):
        if self.subfund_infos is None:
            id_subfund = self.get_shareclass_infos("id_subfund")
            self.subfund_infos = self.fetcher.fetch_subfund_infos(id_subfund)
        
        #TODO: move to processor
        self.subfund_infos["subfund_indicator"] = \
            self.subfund_infos["subfund_code"].iloc[0].split("_")[1] + "-NH"
        
        if info is None:
            return self.subfund_infos
        else:
            return self.subfund_infos[info].iloc[0]
    
    def get_fund_infos(self, info=None):
        if self.fund_infos is None:
            fund_id = self.get_subfund_infos("id_fund")
            self.fund_infos = self.fetcher.fetch_fund_infos(fund_id)

        if info is None:
            return self.fund_infos
        else:
            return self.fund_infos[info].iloc[0]

    def get_instruments(self, info=None, indicator=None):
        # get all instruments associated to the subfund
        id_subfund = self.get_shareclass_infos("id_subfund")
        if self.instruments is None:
            self.instruments = self.fetcher.fetch_instruments(id_subfund)

        self.instruments["hedge_indicator"].fillna(
            self.get_subfund_infos("subfund_indicator"), inplace=True)

        # return all required info of all instruments associated
        # to the required shareclass or group
        if indicator == "all":
            if info is None:
                return self.instruments
            else:
                return self.instruments[info]
        elif indicator is None:
            indicators = self.get_shareclass_infos(["shareclass",
                                                    "shareclass_id"]).tolist()
            indicators.append(self.get_subfund_infos("subfund_indicator"))
            indicator = indicators
        
        if info is None:
            return self.instruments.loc[
                self.instruments["hedge_indicator"].isin(indicator)]
        else:
            return self.instruments.loc[
                self.instruments["hedge_indicator"].isin(indicator),
                info]

    def get_instruments_infos(self, info=None):
        if self.instruments_infos is None:
            instrument_id_list = self.get_instruments().index
            db_instruments_infos = \
                self.fetcher.fetch_db_instruments_infos(instrument_id_list)

            sp_instruments_infos = self.processor.set_sp_instrument_infos()

            self.instruments_infos = sp_instruments_infos.append(db_instruments_infos)
            self.processor.clean_instruments_infos()

        return self.instruments_infos.loc[self.get_instruments().index]
    
    def get_isins_in_group(self, indicator):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_isins_in_group(indicator=indicator,
                                                  id_subfund=id_subfund)

        return isins

    def get_subfund_shareclasses(self):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_subfund_shareclasses(id_subfund)
        
        return isins

    def get_distribution_vector(self, isin):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[isin]
    
    def get_valuation_weight_vector(self, isin):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[isin] / self.get_shareclass_nav("shareclass_total_net_asset_sf_curr")

    def get_distribution_weight(self, isin):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[isin] / self.get_instruments("market_and_accrued_fund")