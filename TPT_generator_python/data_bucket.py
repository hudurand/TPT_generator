import pandas as pd
from .processor import get_sp_instrument_infos

class Data_Bucket():

    def __init__(self,
                 fetcher):

        self.fetcher = fetcher
        self.shareclass_infos = None
        self.shareclass_nav = None
        self.subfund_infos = None
        self.instruments = None
        self.instruments_infos = None

    def get_shareclass_infos(self, info=None, isin=None):
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
        
        sc_curr = self.get_shareclass_infos(isin=isin, info="shareclass_currency")
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
        self.subfund_infos["subfund_indicator"] = self.subfund_infos["subfund_code"].iloc[0].split("_")[1] + "-NH"
        
        if info is None:
            return self.subfund_infos
        else:
            return self.subfund_infos[info].iloc[0]

    def get_instruments(self, info=None, indicator=None):
        # get all instruments associated to the subfund
        id_subfund = self.get_shareclass_infos("id_subfund")
        if self.instruments is None:
            self.instruments = self.fetcher.fetch_instruments(id_subfund)

        # TODO: move to processor?
        # how?
        self.instruments["hedge_indicator"].fillna(self.get_subfund_infos("subfund_indicator"), inplace=True)

        # return all required info of all instruments associated to the required shareclass or group
        if indicator == "all":
            return self.instruments
        elif indicator is None:
            indicators = self.get_shareclass_infos(["shareclass", "shareclass_id"]).tolist()
            indicators.append(self.get_subfund_infos("subfund_indicator"))
            indicator = indicators
        
        if info is None:
            return self.instruments.loc[self.instruments["hedge_indicator"].isin(indicator)]
        else:
            return self.instruments.loc[self.instruments["hedge_indicator"].isin(indicator), info]

    def get_instruments_infos(self, info=None):
        if self.instruments_infos is None:
            instrument_id_list = self.get_instruments().index
            db_instruments_infos = self.fetcher.fetch_instruments_infos(instrument_id_list)
            sp_instruments_infos = get_sp_instrument_infos(
                #client,
                #instruments
                #keys but in a better way pls
            )
            self.instruments_infos = sp_instruments_infos.append(db_instruments_infos)
        
        return self.instruments_infos.loc[
            self.instruments_infos["14_Identification code of the financial instrument"].isin(self.get_instruments().index)]
    