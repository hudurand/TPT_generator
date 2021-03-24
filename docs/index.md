![logo](5d07dc3917bf6193f2e37f9b_logo-AO-long.svg)
# Welcome to AlphaOmega reporting documentation

This documentation details the implementation and methodology adopted to generate each reports.

The reporting tool was built in python to automate the production of TPT reports with the goal to later be extended into a more general framework for automating the production of reports and be integrated into the DEM application. It uses pandas as its main processing engine and access the data it require with SQL.

As the goal is to create a framework for building reporting tools, it was built in a modular fashion following a domain driven design, dedicating a separate module for each logical unit of the domain (SCR, cash-flow, distribution...). The guideline being to write code that is easily maintainable and extendable.

## Overview

- [Developer guide](developer.md)
    - [Production cycle](developer/#production-cycle)
    - [General principles](developer/#general-principles)
    - [Implementation design](developer/#implementation-design)
    - [Architecture overview](developer/#architecture-overview)
- TPT Solvency II
    - [Methodology](methodology.md)
        - [Methodology](methodology/#required-data)
        - [Sections](methodology/#sections)
    - [Implementation](implementation.md)
        - [Modelisation](implementation/#modelisation)
        - [Generation steps](implementation/#generation-steps)
- [Reference](reference.md)

## Commands

* `python ./generate_TPT_report.py generate_from_config <config-file>` - generate one or multiple TPT report from a config file.

## Project layout

```
 |- README.md                          # Installation and usage instructions.
 |- generate_TPT_report.py
 |- run_fetcher.py
 |- run_generator.py
 |- configs/                           # The configuration files.
 |   |- config_BIL.yml
 |   |- config_dynasty.yml
 |   |- ...
 |- TPT_generator_python/              # The TPT generation module.
 |   |- TPT_generator.py  
 |   |  ...
 |- Testing/                           # Unit tests
 |   |  ...
 |- Docs/                              # Documentation
 |   |  ...
```

## TODOS
- [ ] TPT
    - [ ] CQS mapping
    - [ ] Complete SCR module (filling SCR sheet)
- [ ] Extract parent class for DataBucket and Processor
- [ ] Compute actualisation rates
- [ ] Implement quality check 

