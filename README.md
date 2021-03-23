# TPT_generator
TPT production tool

This module contains the python source code to generate TPT reports using datas in database.

## Usage
### Requirements
- anaconda (optional)
- python3
- pandas
- jupyter
- pytest
- fire

### Installation
clone the repository:
```
https://github.com/hudurand/TPT_generator.git
```

Connect to SQL server: (db_fetcher.py l18)
```Python
self.connector = pyodbc.connect('driver={SQL Server};'
                               +'Server=DESKTOP-RGN6M86;'
                               +'Database=intranet;'
                               +'Trusted_Connection=yes;')
```

### TPT generation
### notebooks
### testing

## 