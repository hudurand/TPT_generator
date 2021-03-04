from TPT_generator_python import TPT_Fetcher, Data_Bucket
from pathlib import Path

if __name__ == "__main__":

    DATE = "2020-12-31"
    CLIENT = "Dynasty"
    ISIN = "LU1280365476"
    SOURCE_DIR = Path("./data")

    f = TPT_Fetcher(DATE, CLIENT, ISIN, SOURCE_DIR)
    bucket = Data_Bucket(CLIENT, f)

#    print(f.get_shareclass_infos())
#    print(f.get_subfund_infos())
#    print(f.get_fund_infos())
    #print(f.get_instruments("market_and_accrued_fund"))
#    print(f.get_instruments_infos())
    #group_id = f.get_shareclass_infos("shareclass")
    #print(group_id)
    instruments = bucket.get_instruments()
    #print(instruments)
    infos = bucket.get_shareclass_nav(isin="LU1808855289")
    print(infos)

    #dedicated_group = f.get_isins_in_group(group_id)
    #print(dedicated_group)
    #for code_isin in dedicated_group:
    #    print(f.substract_cash(code_isin, group_id))
    #    #print(f.get_shareclass_nav(isin=code_isin))
