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
        # print("Initiliasing Data Bucket.")
        # Config inputs
        # print("Setting configuration.")
        self.date = date
        self.client = client
        self.shareclass_isin = shareclass_isin
        self.source_dir = source_dir

        # Initialise fetcher
        self.fetcher = TPT_Fetcher(self.date,
                                   self.client,
                                   self.shareclass_isin,
                                   source_dir)

        # Database data placeholders
        self.shareclass_infos = None
        self.shareclass_nav = None
        self.subfund_infos = None
        self.fund_infos = None
        self.instruments = None
        self.instruments_infos = None

        # Processing results placeholders
        self.distribution_matrix = None
        self.processing_data = None
        self.scr = None
        
        # Data processing objects
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

    def init_processing_data(self):
        
        isins = self.shareclass_infos.index
        index = pd.MultiIndex.from_product([self.get_instruments(indicator="all").index,
                                            isins], names=["instrument", "shareclass"])
        self.processing_data = pd.DataFrame(index=index)
        self.processing_data = \
            self.processing_data.join(
                self.get_instruments_infos(info=[FIELDS["12"],
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
                                                 FIELDS["93"]],
                                            instruments="all")) # whole subfund processing don't forget to specify `instruments="all"`
        self.processing_data = \
            self.processing_data.join(
                self.get_instruments(info="market_and_accrued_fund",
                                     indicator="all")) # whole subfund processing don't forget to specify `indicator="all"`

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

        if isin == "all":
            isin = self.shareclass_infos.index.tolist()
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
            for i in self.get_shareclass_infos(isin="all").index.tolist():
                sc_id = self.get_shareclass_infos(info="id", isin=i)
                sc_curr = self.get_shareclass_infos(info="shareclass_currency",
                                                    isin=i)
                nav = self.fetcher.fetch_shareclass_nav(sc_id,
                                                        sc_curr,
                                                        sf_curr,
                                                        self.date)
                
                nav.index = [i]
                self.shareclass_nav = self.shareclass_nav.append(nav)
        #print(self.shareclass_nav)
        if isin is None and info is None:
            return self.shareclass_nav.loc[self.shareclass_isin]
        elif isin is None:
            return self.shareclass_nav.loc[self.shareclass_isin, info]
        elif info is None:
            return self.shareclass_nav.loc[isin]
        else:
            return self.shareclass_nav.loc[isin, info]

    def get_subfund_infos(self, info=None):
        if self.subfund_infos is None:
            id_subfund = self.get_shareclass_infos(info="id_subfund")
            self.subfund_infos = self.fetcher.fetch_subfund_infos(id_subfund)
        
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
        id_subfund = self.get_shareclass_infos(info="id_subfund")
        if self.instruments is None:
            self.instruments = self.fetcher.fetch_instruments(id_subfund,
                                                              self.date)
            self.processor.process_instruments()

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
            indicators = self.get_shareclass_infos(info=["shareclass",
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
            
            # clean and process data
            self.processor.clean_instruments_infos()
            self.processor.process_instruments_infos()
            self.instruments_infos.index.names = ["instrument"]

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

        isins = self.fetcher.fetch_isins_in_group(id_subfund=id_subfund,
                                                  indicator=indicator)

        return isins

    def get_subfund_shareclasses(self):
        id_subfund = self.get_subfund_infos("id")

        isins = self.fetcher.fetch_subfund_shareclasses(id_subfund)
        
        return isins

    def get_distribution_vector(self):
        if self.processing_data is None:
            self.processor.compute_processing_data()

        All = slice(None)
        vec = self.processing_data.loc[(All, self.shareclass_isin), "distribution"]
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
               / self.get_instruments("market_and_accrued_fund")

    def get_processing_data(self, info):
        if self.processing_data is None:
            self.processor.compute_processing_data()
        
        view = self.processing_data.loc[(self.get_instruments().index,
                                         self.shareclass_isin), info]
        view.index = view.index.droplevel("shareclass")

        return view
    
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
        
    def __repr__(self):
        if self.fund_infos is None:
            fund_infos = None
        else:
            fund_infos = f"{self.fund_infos.shape}"
            for idx in self.fund_infos.index.to_list():
                fund_infos += f"""
CODE:                                            {self.fund_infos.loc[idx, "fund_issuer_code"]}
NAME:                                            {self.fund_infos.loc[idx, "fund_name"]}
COUNTRY:                                         {self.fund_infos.loc[idx, "fund_country"]}
...
"""

        if self.subfund_infos is None:
            subfund_infos = None
        else:
            subfund_infos = f"{self.subfund_infos.shape}"
            for idx in self.subfund_infos.index.to_list():
                subfund_infos += f"""
CODE:                                            {self.subfund_infos.loc[idx, "subfund_lei"]}
NAME:                                            {self.subfund_infos.loc[idx, "subfund_name"]}
CURRENCY:                                        {self.subfund_infos.loc[idx, "subfund_currency"]}
ID:                                              {self.subfund_infos.loc[idx, "id"]}
...
"""

        if self.shareclass_infos is None:
            shareclass_infos = None
        else:
            shareclass_infos = f"{self.shareclass_infos.shape}"
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
            shareclass_nav = f"{self.shareclass_nav.shape}"
            for idx in self.shareclass_nav.index.to_list():
                shareclass_nav += f"""
{self.shareclass_infos.loc[idx].name}
NAV DATE:                                        {self.shareclass_nav.loc[idx, "nav_date"]}
TNA in shareclass currency:                      {self.shareclass_nav.loc[idx, "shareclass_total_net_asset_sc_curr"]}
TNA in subfund currency:                         {self.shareclass_nav.loc[idx, "shareclass_total_net_asset_sf_curr"]}
TNA of subfund:                                  {self.shareclass_nav.loc[idx, "subfund_total_net_asset"]}
...
"""

        if self.instruments is None:
            instruments = None
        else:
            instruments = f"""{self.instruments.shape}
TNA of instruments values in subfund currency:   {self.instruments["market_and_accrued_fund"].sum()}

"""

        if self.instruments_infos is None:
            instruments_infos = None
        else:
            instruments_infos = f"{self.instruments_infos.shape}"

        return f"""
    fund infos:              {fund_infos}
    subfund infos:           {subfund_infos}
    shareclass infos:        {shareclass_infos}
    shareclass nav:          {shareclass_nav}
    instruments:             {instruments}
    instruments infos:       {instruments_infos}
        """
        #self.instruments = None
        #self.instruments_infos = None