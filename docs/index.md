![logo](5d07dc3917bf6193f2e37f9b_logo-AO-long.svg)
# Welcome to AlphaOmega reporting documentation

This documentation details the implementation and methodology adopted to generate each reports.

## Commands

* `python ./generate_TPT_report.py generate from config <config-file>` - generate one or multiple TPT report from a config file.

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

## TODOs

- CQS mapping
- Move processing methods that remains in TPT_generator to processor
    - AI 
    - Cash flows
    - SCR
- Complete SCR module (filling SCR sheet)
- Compute actualisation rates
- Refactor processing module
- Implement quality check 