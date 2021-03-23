import fire
import pandas as pd
import pyodbc
import yaml
import os
import logging
from pathlib import Path
from timeit import default_timer as timer

from TPT_generator_python import TPTGenerator

#def generate_TPT_report(date,
#                        client, 
#                        isin,
#                        source_dir,
#                        output_dir):
#
#    print(f"generating report {client} {isin} {date}")
#    start = timer()
#
#    g = TPTGenerator(date,
#                      client,
#                      isin,
#                      source_dir,
#                      output_dir)
#
#    g.create_empty_report()
#    g.generate()
#    g.output_excel()
#    end = timer()
#    print(end - start)

def generate_from_config(config_file, debug=False):
    """
    Generate TPT report(s) using input arguments specified in a config file.

    Args:
        config_file (path): configuration file
        debug (bool): run in debug mode
    """
    # setup logger
    if debug:
        logging.basicConfig(
            format='%(name)-40s - %(levelname)-8s - %(message)s',
            filename='logs/TPT_generator.log',
            filemode='w',
            level=logging.DEBUG)
    else:
        logging.basicConfig(
            format='%(name)-40s - %(levelname)-8s - %(message)s',
            filename='logs/TPT_generator.log',
            filemode='w',
            level=logging.INFO)

    # read config file
    with open(config_file) as cf:
        config = yaml.load(cf.read())
    
    # extract configuration args
    date = pd.to_datetime(config["date"]).date()
    source_dir = Path(config["source_dir"])
    client = config["client"]
    sym_adj = config["symmetric_adjustment"]
    out_dir = Path(config["output_dir"])
    if not out_dir.exists():
        os.makedirs(out_dir)
    
    # instantiate generator(s)
    TPT_config = config["reports"]["TPT"]
    TPT = TPTGenerator(date,
                       client,
                       out_dir,
                       source_dir,
                       sym_adj)
    isins = TPT_config["shareclasses"]
    TPT.generate(isins)

if __name__ == "__main__":
    fire.Fire()