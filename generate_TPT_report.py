import fire
import pandas as pd
import pyodbc
import yaml
import os
from pathlib import Path
from timeit import default_timer as timer

from TPT_generator_python import TPT_Generator

def generate_TPT_report(date,
                        client, 
                        isin,
                        source_dir,
                        output_dir):

    print(f"generating report {client} {isin} {date}")
    start = timer()

    g = TPT_Generator(date,
                      client,
                      isin,
                      source_dir,
                      output_dir)

    g.create_empty_report()
    g.generate()
    g.output_excel()
    end = timer()
    print(end - start)

def generate_from_config(config_file):
    with open(config_file) as cf:
        config = yaml.load(cf.read())
    #print(config)
    date = pd.to_datetime(config["date"]).date()
    #print(date)
    source_dir = config["source_dir"]
    #print(source_dir)
    clients = config["clients"]
    #print(clients)
    for client, prod in clients.items():
        out_dir = Path(prod["output_dir"])
        if not out_dir.exists():
            os.makedirs(out_dir)
        for isin in prod["shareclasses"]:
            generate_TPT_report(date,
                                client,
                                isin,
                                source_dir,
                                out_dir)

if __name__ == "__main__":
    fire.Fire()