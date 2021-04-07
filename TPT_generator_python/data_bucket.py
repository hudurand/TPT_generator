import pandas as pd
import numpy as np
from .db_fetcher import TPTFetcher
from .processor import DataProcessor
from .scr_module import SCRModule
from .constants import DB_INSTRUMENTS_INFOS_MAP, FIELDS, ALL
import logging

class DataBucket():
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
                 source_dir,
                 shareclass_isin=None):
        # print("Initiliasing Data Bucket.")
        # Config inputs
        # print("Setting configuration.")
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initialising bucket...")

        self.date = date
        self.client = client
        self.source_dir = source_dir
        self.shareclass_isin = shareclass_isin

        # Initialise fetcher
        self.fetcher = TPTFetcher(source_dir)

        # Database data placeholders
        self.shareclass_infos = None
        self.shareclass_nav = None
        self.currency_rate = None
        self.subfund_infos = None
        self.fund_infos = None
        self.instruments = None
        self.instruments_infos = None
        self.group_map = None

        # Processing results placeholders
        self.distribution_matrix = None
        self.processing_data = None
        self.scr = None
        
        # Data processing objects
        self.processor = DataProcessor(self)
        self.scr_module = SCRModule(self)

        self.logger.info("Bucket initialised")
        self.logger.debug(f"data_bucket state: {self}")
       
    def fetch(self):
        """
        Run all data-acquiring methods at once and each of them will in turn
        call their corresponding fetching methods to get data from database.
        """
        #TODO replace by fetching functions

        #self.get_shareclass_infos()
        #self.get_shareclass_nav()
        #self.get_subfund_infos()
        #self.get_fund_infos()
        #self.get_instruments()
        #self.get_instruments_infos()

    def update(self, isin):
        # if bucket doesn't have any data:
        #   set new isin as attribute and fetch data
        # if bucket already have data of a subfund and new isin is in subfund
        #   only need to change attribute (and recompute SCR)
        # if bucket already have data but new isin is not in subfund
        #   data must be reset, refetched and recomputed.

        # BUT!!! in fact we only need to consider te case of a shareclass not in 
        # the current subfund, since missing data is fetched dynamically.
        
        self.logger.info("Updating shareclass to generate report for")
        self.shareclass_isin = isin
        if self.shareclass_infos is not None:
            # SCR requires to be reset at each new shareclass
            self.scr = None

            if self.shareclass_isin not in self.shareclass_infos.index:
                # reset database data placeholders
                self.shareclass_infos = None
                self.shareclass_nav = None
                self.subfund_infos = None
                self.fund_infos = None
                self.instruments = None
                self.instruments_infos = None
                self.group_map = None

                # reset processing results placeholders
                self.distribution_matrix = None
                self.processing_data = None

            else:
                assert self.get_shareclass_infos().name == self.shareclass_isin
                assert self.get_shareclass_nav().name == self.shareclass_isin
                assert self.get_subfund_infos("id") == self.get_shareclass_infos("id_subfund")
                # by this point, we can be sure that the data bucket is correctly set.


    def init_processing_data(self):
        self.logger.info("Initialising processing data placeholder")
        isins = self.get_shareclass_infos(isin=ALL).index
        index = pd.MultiIndex.from_product([self.get_instruments_by_index(idx=ALL).index,
                                            isins], names=["instrument", "shareclass"])
        self.processing_data = pd.DataFrame(index=index)
        self.processing_data = \
            self.processing_data.join(
                self.get_instruments_infos(idx=ALL,
                                           info=[FIELDS["12"],
                                                 FIELDS["20"],
                                                 FIELDS["21"],
                                                 FIELDS["39"],
                                                 FIELDS["53"],
                                                 FIELDS["55"],
                                                 FIELDS["59"],
                                                 FIELDS["61"],
                                                 FIELDS["62"],
                                                 FIELDS["71"],
                                                 FIELDS["72"],
                                                 FIELDS["90"],
                                                 FIELDS["93"]]))
        self.processing_data = \
            self.processing_data.join(
                self.get_instruments_by_index(idx=ALL,
                                              info="market_value_fund"))
        self.processing_data = \
            self.processing_data.join(
                self.get_shareclass_nav(info="shareclass_total_net_asset_sc_ccy", isin=ALL))
        
        self.logger.debug(f"data_bucket state: {self}")

    def get_shareclass_infos(self, info=None, isin=None):
        """
        Feed shareclass related informations.
        Args:
            info ([str, list?]): name of the information to return, \
                (name used in database).
            isin (str): isin code of the shareclass, if none the isin stored as\
                attribute of the fetcher is used.
        """
        if self.shareclass_infos is None:
            self.shareclass_infos = self.fetcher.fetch_shareclass_infos(self.shareclass_isin)
            self.shareclass_infos.set_index("code_isin", inplace=True)
            other_isins = self.get_subfund_shareclasses()
            other_isins.remove(self.shareclass_isin)
            self.shareclass_infos = self.shareclass_infos.append(
                self.fetcher.fetch_shareclass_infos(
                    other_isins).set_index("code_isin"))

            self.logger.debug(f"data_bucket state: {self}")

        if isin is None and info is None:
            return self.shareclass_infos.loc[self.shareclass_isin]
        elif isin is None:
            return self.shareclass_infos.loc[self.shareclass_isin,
                                             info]
        elif info is None:
            return self.shareclass_infos.loc[isin]
        else:
            return self.shareclass_infos.loc[isin,
                                             info]

    def get_shareclass_nav(self, info=None, isin=None):
        sf_curr = self.get_subfund_infos("subfund_currency")

        if self.shareclass_nav is None:
            self.shareclass_nav = pd.DataFrame()
            for i in self.get_shareclass_infos(isin=ALL).index.tolist():
                sc_id = self.get_shareclass_infos(info="id", isin=i)
                sc_curr = self.get_shareclass_infos(info="shareclass_currency",
                                                    isin=i)
                nav = self.fetcher.fetch_shareclass_nav(sc_id,
                                                        sc_curr,
                                                        sf_curr,
                                                        self.date)
                nav["rate"] = nav["shareclass_total_net_asset_sc_ccy"] \
                            / nav["shareclass_total_net_asset_sf_ccy"]
                nav.index = [i]
                self.shareclass_nav = self.shareclass_nav.append(nav)
            self.shareclass_nav.index.name = "shareclass"
            
            self.logger.debug(f"data_bucket state: {self}")

        #print(self.shareclass_nav)
        if isin is None and info is None:
            return self.shareclass_nav.loc[self.shareclass_isin]
        elif isin is None:
            return self.shareclass_nav.loc[self.shareclass_isin, info]
        elif info is None:
            return self.shareclass_nav.loc[isin]
        else:
            return self.shareclass_nav.loc[isin, info]

    def get_groups(self, isin=None):
        if self.group_map is None:
            id_list = self.get_shareclass_infos("id", ALL).tolist()
            self.group_map = self.fetcher.fetch_group_map(id_list)
        
        shareclass_id = self.get_shareclass_infos("id", isin)
        return self.group_map.loc[self.group_map["shareclass_id"]==shareclass_id,
                                                 "group_id"]
    
    def get_isins_in_group(self, id_group):
        if self.group_map is None:
            id_list = self.get_shareclass_infos("id", ALL).tolist()
            self.group_map = self.fetcher.fetch_group_map(id_list)

        shareclass_ids = self.group_map.loc[self.group_map["group_id"]==id_group,
                                            "shareclass_id"]

        return self.shareclass_infos.loc[self.shareclass_infos["id"].isin(shareclass_ids)].index

    def get_subfund_shareclasses(self):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_subfund_shareclasses(id_subfund)
        
        return isins

    def get_subfund_infos(self, info=None):
        if self.subfund_infos is None:
            id_subfund = self.get_shareclass_infos(info="id_subfund")
            self.subfund_infos = self.fetcher.fetch_subfund_infos(id_subfund)
            
            self.logger.debug(f"data_bucket state: {self}")
        
        #TODO: move to processor?
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

            self.logger.debug(f"data_bucket state: {self}")

        if info is None:
            return self.fund_infos
        else:
            return self.fund_infos[info].iloc[0]

    def get_instruments_by_index(self,
                                 idx,
                                 info=ALL):
        # get all instruments associated to the subfund
        id_subfund = self.get_shareclass_infos(info="id_subfund")
        if self.instruments is None:
            self.instruments = self.fetcher.fetch_instruments(id_subfund,
                                                              self.date)
            self.processor.process_instruments()

            self.logger.debug(f"data_bucket state: {self}")

        return self.instruments.loc[idx, info]
        
    def get_instruments_by_group(self,
                                 info=None,
                                 group=None):
        instrument_groups = self.get_instruments_by_index(idx=ALL, info="id_group")
        index = instrument_groups.loc[instrument_groups.isin(group)].index

        return self.get_instruments_by_index(index, info)

    def get_instruments(self, 
                        info=ALL):
        """
        Returns all required info of all instruments associated to the required
        shareclass or group.

        Args:
            info (str or [str]): required infos.
            indicator (str, [str]]): shareclass grouping indicator.
            index (str, [str]): indexes of required instruments.

        Returns:
            pandas.DataFrame: selected infos (if None: all) of the instruments
            associated to the selected grouping indicators (if None: instruments
            associated to the current shareclass).
        """

        # if indicator is None, return the instruments associated to the current
        # shareclass (i.e: self.shareclass_isin)
        # the corresponding instruments are those associated to the shareclass 
        # itself, to any group the shareclass belongs to and to the whole subfund.
        groups = self.get_groups()                                        
        return self.get_instruments_by_group(info, groups)

    def get_instruments_infos(self, idx=None, info=ALL):
        """
        Feed instruments infos, both data and processed.

        Args:
            info ([str, [str], None]): required infos.
            instruments ([str, [str], None]): required instruments index
        """
        if self.instruments_infos is None:
            self.logger.info("fetching instruments infos")
            instrument_id_list = self.get_instruments_by_index(idx=ALL).index
            db_instruments_infos = \
                self.fetcher.fetch_db_instruments_infos(instrument_id_list)

            sp_instruments_infos = self.processor.set_sp_instrument_infos()

            self.instruments_infos = sp_instruments_infos.append(db_instruments_infos)
            # clean and process data
            self.processor.clean_instruments_infos()
            self.processor.process_instruments_infos()
            self.instruments_infos.index.names = ["instrument"]

            self.logger.debug(f"data_bucket state: {self}")

        if idx is None:
            return self.instruments_infos.loc[self.get_instruments().index,
                                              info]
        else:
            return self.instruments_infos.loc[idx, info]

    def get_distribution_vector(self):
        if self.processing_data is None:
            self.logger.info("Computing processing data")
            self.processor.compute_processing_data()

            self.logger.debug(f"data_bucket state: {self}")

        vec = self.processing_data.loc[(ALL, self.shareclass_isin), "distribution"]
        vec = vec.droplevel("shareclass")
        return vec
#    
#    def get_valuation_weight_vector(self):
#        if self.processing_data is None:
#            self.processor.compute_processing_data()
#
#        All = slice(None)
#        vec = self.processing_data.loc[(All, self.shareclass_isin), "valuation weight"]
#        vec = vec.droplevel("shareclass")
#        return vec
#
    def get_distribution_weight(self):
        return self.get_distribution_vector() \
               / self.get_instruments("market_value_fund")

    def get_processing_data(self, info):
        if self.processing_data is None:
            self.processor.compute_processing_data()
        
        view = self.processing_data.loc[(self.get_instruments().index,
                                         self.shareclass_isin), info]
        view.index = view.index.droplevel("shareclass")

        return view
    
    def get_scr_results(self, submodule):
        self.scr = pd.DataFrame(np.nan,
                                index=self.get_instruments().index,
                                columns=[FIELDS["97"],
                                         FIELDS["98"],
                                         FIELDS["99"],
                                         FIELDS["100"],
                                         FIELDS["102"],
                                         FIELDS["105a"],
                                         FIELDS["105b"]])    
        self.scr_module.compute_scr()

        return self.scr[submodule]
        
    def __repr__(self):
        if self.fund_infos is None:
            fund_infos = None
        else:
            fund_infos = f"{self.fund_infos.shape}\n"
            for idx in self.fund_infos.index.to_list():
                fund_infos += f"""
NAME:                                            {self.fund_infos.loc[idx, "fund_name"]}
COUNTRY:                                         {self.fund_infos.loc[idx, "fund_country"]}
...
"""
#CODE:                                            {self.fund_infos.loc[idx, "fund_issuer_code"]}

        if self.subfund_infos is None:
            subfund_infos = None
        else:
            subfund_infos = f"{self.subfund_infos.shape}\n"
            subfund_infos += f"""
CODE:                                            {self.subfund_infos.loc[0, "subfund_lei"]}
NAME:                                            {self.subfund_infos.loc[0, "subfund_name"]}
CURRENCY:                                        {self.subfund_infos.loc[0, "subfund_currency"]}
ID:                                              {self.subfund_infos.loc[0, "id"]}
...
"""

        if self.shareclass_infos is None:
            shareclass_infos = None
        else:
            shareclass_infos = f"{self.shareclass_infos.shape}\n"
            for idx in self.shareclass_infos.index.to_list():
                shareclass_infos += f"""
ISIN:                                            {self.shareclass_infos.loc[idx].name}
NAME:                                            {self.shareclass_infos.loc[idx, "shareclass_name"]}
CURRENCY:                                        {self.shareclass_infos.loc[idx, "shareclass_currency"]}
...
"""

        if self.shareclass_nav is None:
            shareclass_nav = None
        else:
            shareclass_nav = f"{self.shareclass_nav.shape}\n"
            for idx in self.shareclass_nav.index.to_list():
                shareclass_nav += f"""
{self.shareclass_infos.loc[idx].name}
NAV DATE:                                        {self.shareclass_nav.loc[idx, "nav_date"]}
TNA in shareclass currency:                      {self.shareclass_nav.loc[idx, "shareclass_total_net_asset_sc_ccy"]}
TNA in subfund currency:                         {self.shareclass_nav.loc[idx, "shareclass_total_net_asset_sf_ccy"]}
TNA of subfund:                                  {self.shareclass_nav.loc[idx, "subfund_total_net_asset"]}
...
"""

        if self.instruments is None:
            instruments = None
        else:
            instruments = f"""{self.instruments.shape}
TNA of instruments values in subfund currency:   {self.instruments["market_value_fund"].sum()}

"""

        if self.instruments_infos is None:
            instruments_infos = None
        else:
            instruments_infos = f"{self.instruments_infos.shape}\n"

        return f"""
    fund infos:              {fund_infos}
    subfund infos:           {subfund_infos}
    shareclass infos:        {shareclass_infos}
    shareclass nav:          {shareclass_nav}
    instruments:             {instruments}
    instruments infos:       {instruments_infos}
        """
 