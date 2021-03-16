from TPT_generator_python import TPT_Fetcher, Data_Bucket
from pathlib import Path

if __name__ == "__main__":

    DATE = "2020-12-31"
    CLIENT = "BIL"
    ISIN = "LU1689730718"
    SOURCE_DIR = Path("./data")

    bucket = Data_Bucket(DATE, CLIENT, ISIN, SOURCE_DIR)

    bucket.fetch()
    print(bucket.get_processing_data("distribution"))
    #bucket.init_processing_data()
    #print(bucket.processing_data)
