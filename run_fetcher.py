from TPT_generator_python import TPT_Fetcher
from pathlib import Path

if __name__ == "__main__":

    DATE = "2020-12-31"
    CLIENT = "BIL"
    ISIN = "LU1689732417"
    SOURCE_DIR = Path("./data")

    f = TPT_Fetcher(DATE, CLIENT, ISIN, SOURCE_DIR)

    #print(f.get_shareclass_infos("code_isin"))
    #print(f.get_subfund_infos("id"))
    print(f.get_instruments_infos())
    #print(f.get_instruments_infos().loc[f.get_instruments_infos()['14_Identification code of the financial instrument'] == 'CH0559601544', "93_Sensitivity to underlying asset price (delta)"])
    #print(f.get_instruments().loc["286_2", ["market_and_accrued_fund", "market_fund", "accrued_fund"]])
    #print(f.get_instruments().loc["286_2", ["market_and_accrued_asset", "market_asset", "accrued_asset"]])
    #print(f.get_instruments().loc["581_2", ["market_and_accrued_fund", "market_fund", "accrued_fund"]])