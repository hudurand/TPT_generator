import fire
import pandas as pd
import pyodbc
from TPT_generator_python import TPT_Generator
from timeit import default_timer as timer

def generate_TPT_report(client, 
                        isin,
                        date):

    print(f"generating report {client} {isin} {date}")
    start = timer()

    g = TPT_Generator(date,
                      client,
                      isin)

    g.create_empty_report()
    g.generate()
    g.output_excel()
    end = timer()
    print(end - start)

if __name__ == "__main__":
    fire.Fire()