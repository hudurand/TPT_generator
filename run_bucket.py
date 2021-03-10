from TPT_generator_python import TPT_Fetcher, Data_Bucket
from pathlib import Path

if __name__ == "__main__":

    DATE = "2020-12-31"
    CLIENT = "BIL"
    ISIN = "LU1689732417"
    SOURCE_DIR = Path("./data")

    bucket = Data_Bucket(DATE, CLIENT, ISIN, SOURCE_DIR)

    bucket.fetch()
    print(bucket.get_instruments_infos("59_Credit quality step"))
