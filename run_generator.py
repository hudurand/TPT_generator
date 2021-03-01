from TPT_generator_python import TPT_Generator
from timeit import default_timer as timer
import pandas as pd

if __name__ == "__main__":
    DATE = pd.to_datetime("2020-12-31").date()
    CLIENT = "BIL"
    #ISIN = #"LU1689729546"
    ISIN = "LU1689732417"
           #"LU1689729546"
           #"LU1689729629"
           #"LU1808854803"
           #"LU1689730122"
           #"LU1689730718"
           #"LU1689730809"

    SOURCE_DIR = './data'
    OUTPUT_DIR = './production/Dynasty'

    start = timer()
    g = TPT_Generator(DATE,
                      CLIENT,
                      ISIN,
                      SOURCE_DIR,
                      OUTPUT_DIR)

    #g.fill_column_19()
    #print(g.cash_flows)
    #{g.create_empty_report()
    g.generate()
    #g.output_excel()
    #print(g.TPT_report[g.fields["19"]])
    end = timer()
    print(end - start)