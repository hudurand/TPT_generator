import pandas as pd
import numpy as np
from .db_fetcher import TPT_Fetcher
from .processor import Data_Processor
from .scr_module import SCR_Module
from .constants import DB_INSTRUMENTS_INFOS_MAP, FIELDS

class Data_Bucket():
    """
    Container class to hold the data coming from the database as well as
    resulting preprocessing in a semi-structured way and feed them to the 
    generator for generation of the report.

    It relies on a fetcher object to get the data from the database and a 
    processor object to apply transformations to the data.
    """

    def __init__(self,
                 date,
                 client,
                 shareclass_isin,
                 source_dir):

        self.date = date
        self.client = client
        self.shareclass_isin = shareclass_isin

        self.fetcher = TPT_Fetcher(self.date,
                                   self.client,
                                   self.shareclass_isin,
                                   source_dir)

        self.shareclass_infos = None
        self.shareclass_nav = None
        self.subfund_infos = None
        self.fund_infos = None
        self.instruments = None
        self.instruments_infos = None
        self.distribution_matrix = None
        self.scr = None

        self.processor = Data_Processor(self)
        self.scr_module = SCR_Module(self)
    
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
            self.shareclass_infos = self.fetcher.fetch_shareclass_infos(self.shareclass_isin)

        if isin is None and info is None:
            return self.shareclass_infos
        elif isin is None:
            return self.shareclass_infos[info].iloc[0]
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
                                                                    sf_curr,
                                                                    self.date)
        
        #print("isin: ", isin)
        #print("info: ", info)

        if isin is None and info is None:
            return self.shareclass_nav
        elif info is None:
            return self.fetcher.fetch_shareclass_nav(sc_id,
                                                     sc_curr,
                                                     sf_curr,
                                                     self.date)
        else:
            return self.fetcher.fetch_shareclass_nav(sc_id,
                                                     sc_curr,
                                                     sf_curr,
                                                     self.date)[info].iloc[0]

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
        """
        Returns all required info of all instruments associated to the required
        shareclass or group.

        Args:
            info (str or [str]): required infos.
            indicator (["all", str, [str]]): shareclass grouping indicator.

        Returns:
            pandas.DataFrame: selected infos (if None: all) of the instruments
            associated to the selected grouping indicators (if None: instruments
            associated to the current shareclass).
        """
        # get all instruments associated to the subfund
        id_subfund = self.get_shareclass_infos("id_subfund")
        if self.instruments is None:
            self.instruments = self.fetcher.fetch_instruments(id_subfund,
                                                              self.date,
                                                              self.client)

        # fill missing value (for BIL)
        # TODO: remove when bdd has been updated
        self.instruments["hedge_indicator"].fillna(
            self.get_subfund_infos("subfund_indicator"), inplace=True)

        # if indicator is "all" return the required infos for all instruments
        # of the subfund.
        if indicator == "all":
            if info is None:
                return self.instruments
            else:
                return self.instruments[info]

        # if indicator is None, return the instruments associated to the current
        # shareclass (i.e: self.shareclass_isin)
        # the corresponding instruments are those associated to the shareclass 
        # itself, to any group the shareclass belongs to and to the whole subfund.
        elif indicator is None:
            indicators = self.get_shareclass_infos(["shareclass",
                                                    "shareclass_id"]).tolist()
            indicators.append(self.get_subfund_infos("subfund_indicator"))
            indicator = indicators

        # return selected infos
        if info is None:
            return self.instruments.loc[
                self.instruments["hedge_indicator"].isin(indicator)]
        else:
            return self.instruments.loc[
                self.instruments["hedge_indicator"].isin(indicator),
                info]

    def get_instruments_infos(self, info=None, instruments=None):
        """
        Feed instruments infos, both data and processed.

        Args:
            info ([str, [str], None]): required infos.
            instruments (["all", str, [str], None]): required instruments index
        """

        if self.instruments_infos is None:
            instrument_id_list = self.get_instruments(indicator="all").index
            db_instruments_infos = \
                self.fetcher.fetch_db_instruments_infos(instrument_id_list)

            sp_instruments_infos = self.processor.set_sp_instrument_infos()

            self.instruments_infos = sp_instruments_infos.append(db_instruments_infos)
            self.processor.clean_instruments_infos()
            # process data if needed
            self.processor.process_instruments()

        if instruments is None and info is None:
            return self.instruments_infos.loc[self.get_instruments().index]
        elif instruments is None:
            return self.instruments_infos.loc[self.get_instruments().index,
                                              info]
        elif instruments=="all":
            if info is None:
                return self.instruments_infos.loc[self.get_instruments(indicator="all").index]
            else:
                return self.instruments_infos.loc[self.get_instruments(indicator="all").index,
                                                  info]
        elif isinstance(instruments, list):
            if info is None:
                return self.instruments_infos.loc[instruments]
            else:
                return self.instruments_infos.loc[instruments,
                                                  info]
        else:
            if info is None:
                return self.instruments_infos.loc[instruments]
            else:
                return self.instruments_infos.loc[instruments,
                                                  info].iloc[0]

                                                
    def get_isins_in_group(self, indicator):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_isins_in_group(indicator=indicator,
                                                  id_subfund=id_subfund)

        return isins

    def get_subfund_shareclasses(self):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_subfund_shareclasses(id_subfund)
        
        return isins

    def get_distribution_vector(self):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[self.shareclass_isin]
    
    def get_valuation_weight_vector(self):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[self.shareclass_isin] \
               / self.get_shareclass_nav("shareclass_total_net_asset_sf_curr")

    def get_distribution_weight(self):
        if self.distribution_matrix is None:
            self.distribution_matrix = self.processor.compute_distribution_matrix()

        return self.distribution_matrix[self.shareclass_isin] \
               / self.get_instruments("market_and_accrued_fund")
    
    def get_SCR_results(self, submodule):
        self.scr = pd.DataFrame(np.nan,
                                index=self.get_instruments().index,
                                columns=[FIELDS["97"],
                                         FIELDS["98"],
                                         FIELDS["99"],
                                         FIELDS["100"],
                                         FIELDS["102"],
                                         FIELDS["105a"],
                                         FIELDS["105b"]])    
        self.scr_module.compute_SCR()

        return self.scr[submodule]
