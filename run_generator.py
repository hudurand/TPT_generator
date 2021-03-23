from TPT_generator_python import TPTGenerator
from timeit import default_timer as timer
from pathlib import Path
import pandas as pd

if __name__ == "__main__":
    DATE = pd.to_datetime("2020-12-31").date()
    CLIENT = "Pictet"
    ISIN = "LU1787059465"
    SOURCE_DIR = Path('./data')
    OUTPUT_DIR = Path('./production/Dynasty')

    start = timer()
    g = TPTGenerator(DATE,
                      CLIENT,
                      ISIN,
                      SOURCE_DIR,
                      OUTPUT_DIR)

    g.generate()
    print(g)
    g.output_excel()

    end = timer()
    print(end - start)