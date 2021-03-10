import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from pandas.testing import assert_series_equal, assert_index_equal

from TPT_generator_python import TPT_Fetcher
from TPT_generator_python import TPT_Generator
from TPT_generator_python import Data_Bucket

@pytest.fixture(scope="module",
    params=[
        pytest.param(("BIL", "LU1689732417", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD Corporate Investment Grade - P USD CAP_LU1689732417_20201231.xlsx"), id="LU1689732417_BIL"),
        pytest.param(("BIL", "LU1689729546", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Absolute Return - I EUR CAP_LU1689729546_20201231.xlsx"), id="LU1689729546_BIL"),
        pytest.param(("BIL", "LU1689729629", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Absolute Return - P EUR CAP_LU1689729629_20201231.xlsx"), id="LU1689729629_BIL"),
        pytest.param(("BIL", "LU1808854803", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds Emerging Markets - I EUR Hedged CAP_LU1808854803_20201231.xlsx"), id="LU1808854803_BIL"),
        pytest.param(("BIL", "LU1689730122", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds Emerging Markets - I USD CAP_LU1689730122_20201231.xlsx"), id="LU1689730122_BIL"),
        pytest.param(("BIL", "LU1689730718", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds EUR Corporate Investment Grade - I EUR CAP_LU1689730718_20201231.xlsx"), id="LU1689730718_BIL"),
        pytest.param(("BIL", "LU1689730809", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds EUR Corporate Investment Grade - P EUR CAP_LU1689730809_20201231.xlsx"), id="LU1689730809_BIL"),
        #pytest.param(("BIL", "LU1689731286", "2020-12-30", "AO_TPT_V5.0_BIL Invest - Bonds EUR High Yield - I EUR CAP_LU1689731286_20201231.xlsx"), id="LU1689731286_BIL"),
        #pytest.param(("BIL", "LU1689731872", "2020-12-30", "AO_TPT_V5.0_BIL Invest - Bonds EUR Sovereign - I EUR CAP_LU1689731872_20201231.xlsx"), id="LU1689731872_BIL"),
        #pytest.param(("BIL", "LU1689731955", "2020-12-30", "AO_TPT_V5.0_BIL Invest - Bonds EUR Sovereign - P EUR CAP_LU1689731955_20201231.xlsx"), id="LU1689731955_BIL"),
        pytest.param(("BIL", "LU1565452015", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds Renta Fund - P EUR CAP_LU1565452015_20201231.xlsx"), id="LU1565452015_BIL"),
        pytest.param(("BIL", "LU1808854985", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD Corporate Investment Grade - I EUR Hedged CAP_LU1808854985_20201231.xlsx"), id="LU1808854985_BIL"),
        pytest.param(("BIL", "LU1689732334", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD Corporate Investment Grade - I USD CAP_LU1689732334_20201231.xlsx"), id="LU1689732334_BIL"),
        pytest.param(("BIL", "LU1808855016", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD High Yield - I EUR Hedged CAP_LU1808855016_20201231.xlsx"), id="LU1808855016_BIL"),
        #pytest.param(("BIL", "LU1689733498", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD Sovereign - I USD CAP_LU1689733498_20201231.xlsx"), id="LU1689733498_BIL"),
        #pytest.param(("BIL", "LU1917566066", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Bonds USD Sovereign - P EUR Hedged Cap_LU1917566066_20201231.xlsx"), id="LU1917566066_BIL"),
        pytest.param(("BIL", "LU1689734462", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Emerging Markets - I USD CAP_LU1689734462_20201231.xlsx"), id="LU1689734462_BIL"),
        pytest.param(("BIL", "LU1689734546", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Emerging Markets - P USD CAP_LU1689734546_20201231.xlsx"), id="LU1689734546_BIL"),
        pytest.param(("BIL", "LU1689735196", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Europe - I EUR CAP_LU1689735196_20201231.xlsx"), id="LU1689735196_BIL"),
        pytest.param(("BIL", "LU1689735279", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Europe - P EUR CAP_LU1689735279_20201231.xlsx"), id="LU1689735279_BIL"),
        pytest.param(("BIL", "LU1689733902", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Japan - I JPY CAP_LU1689733902_20201231.xlsx"), id="LU1689733902_BIL"),
        pytest.param(("BIL", "LU1689734033", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities Japan - P JPY CAP_LU1689734033_20201231.xlsx"), id="LU1689734033_BIL"),
        pytest.param(("BIL", "LU1689735600", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities US - I USD CAP_LU1689735600_20201231.xlsx"), id="LU1689735600_BIL"),
        pytest.param(("BIL", "LU1689735782", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Equities US - P USD CAP_LU1689735782_20201231.xlsx"), id="LU1689735782_BIL"),
        pytest.param(("BIL", "LU0509288378", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Defensive - P EUR CAP_LU0509288378_20201231.xlsx"), id="LU0509288378_BIL"),
        pytest.param(("BIL", "LU0049912065", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial High - P EUR CAP_LU0049912065_20201231.xlsx"), id="LU0049912065_BIL"),
        pytest.param(("BIL", "LU0548495596", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Low - I EUR CAP_LU0548495596_20201231.xlsx"), id="LU0548495596_BIL"),
        pytest.param(("BIL", "LU0049911091", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Low - P EUR CAP_LU0049911091_20201231.xlsx"), id="LU0049911091_BIL"),
        pytest.param(("BIL", "LU0049910796", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Low - P EUR DIS_LU0049910796_20201231.xlsx"), id="LU0049910796_BIL"),
        pytest.param(("BIL", "LU1033871838", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Low - P USD Hedged CAP_LU1033871838_20201231.xlsx"), id="LU1033871838_BIL"),
        pytest.param(("BIL", "LU0108482372", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Medium - P EUR CAP_LU0108482372_20201231.xlsx"), id="LU0108482372_BIL"),
        pytest.param(("BIL", "LU1440060207", "2020-12-31", "AO_TPT_V5.0_BIL Invest - Patrimonial Medium - P USD Hedged CAP_LU1440060207_20201231.xlsx"), id="LU1440060207_BIL"),
        #pytest.param(("BIL", "LU0698523601", "2020-12-31", "AO_TPT_V5.0_Private One - Capital Balanced Fund - B EUR_LU0698523601_20201231.xlsx"), id="LU0698523601_BIL"),
        #pytest.param(("BIL", "LU2073848363", "2020-12-31", "AO_TPT_V5.0_Private One - Saint-Saens - EUR CAP_LU2073848363_20201231.xlsx"), id="LU2073848363_BIL"),
        pytest.param(("Dynasty", "LU2133138276", "2020-12-31", "AO_TPT_V5.0_Dynasty Corporate Bonds 0 - 2.5 Class A_LU2133138276_20201231.xlsx"), id="LU2133138276_Dynasty"),
        pytest.param(("Dynasty", "LU2133138433", "2020-12-31", "AO_TPT_V5.0_Dynasty Corporate Bonds 0 - 2.5 Class B_LU2133138433_20201231.xlsx"), id="LU2133138433_Dynasty"),
        #pytest.param(("Dynasty", "LU1280365476", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles A CHF_LU1280365476_20201231.xlsx"), id="LU1280365476_Dynasty"),
        #pytest.param(("Dynasty", "LU1280365393", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles A EUR_LU1280365393_20201231.xlsx"), id="LU1280365393_Dynasty"),
        #pytest.param(("Dynasty", "LU1280365559", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles A USD_LU1280365559_20201231.xlsx"), id="LU1280365559_Dynasty"),
        #pytest.param(("Dynasty", "LU1483663818", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles B CHF_LU1483663818_20201231.xlsx"), id="LU1483663818_Dynasty"),
        #pytest.param(("Dynasty", "LU1280365633", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles B EUR_LU1280365633_20201231.xlsx"), id="LU1280365633_Dynasty"),
        #pytest.param(("Dynasty", "LU1840818220", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles B GBP_LU1840818220_20201231.xlsx"), id="LU1840818220_Dynasty"),
        #pytest.param(("Dynasty", "LU1586705938", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles B USD_LU1586705938_20201231.xlsx"), id="LU1586705938_Dynasty"),
        #pytest.param(("Dynasty", "LU1508332993", "2020-12-31", "AO_TPT_V5.0_Dynasty Global Convertibles D EUR_LU1508332993_20201231.xlsx"), id="LU1508332993_Dynasty"),
        #pytest.param(("Dynasty", "LU1073011352", "2020-12-31", "AO_TPT_V5.0_Dynasty High Yield 2026 A EUR_LU1073011352_20201231.xlsx"), id="LU1073011352_Dynasty"),
        #pytest.param(("Dynasty", "LU1586707801", "2020-12-31", "AO_TPT_V5.0_Dynasty High Yield 2026 A USD_LU1586707801_20201231.xlsx"), id="LU1586707801_Dynasty"),
        #pytest.param(("Dynasty", "LU1073013564", "2020-12-31", "AO_TPT_V5.0_Dynasty High Yield 2026 B EUR_LU1073013564_20201231.xlsx"), id="LU1073013564_Dynasty"),
        #pytest.param(("Dynasty", "LU1280365120", "2020-12-31", "AO_TPT_V5.0_Dynasty High Yield 2026 D EUR_LU1280365120_20201231.xlsx"), id="LU1280365120_Dynasty"),
        ])


def params_fixt(request):
    CLIENT = request.param[0]
    ISIN = request.param[1]
    DATE = request.param[2]
    FILE_NAME = request.param[3]

    return CLIENT, ISIN, DATE, FILE_NAME

@pytest.fixture(scope="module")
def reference_TPT_report(params_fixt):
    """
    load the reference TPT report
    """ 

    root_path = Path("./data/reference_reports")
    file_name = params_fixt[3]

 
    report = pd.read_excel(root_path / file_name,
                           sheet_name="Report")
    
    #report.drop(["17b_Asset / Liability",], axis=1, inplace=True)

    report.rename(columns={
        "4_Portfolio currency ( B )": 
            "4_Portfolio currency (B)",
        "20_Contract size for derivatives ": 
            "20_Contract size for derivatives",
        "22_Market valuation in quotation currency  ( A ) ": 
            "22_Market valuation in quotation currency (A)",
        "24_Market valuation in portfolio currency  (B) ":
            "24_Market valuation in portfolio currency (B)",
        "29_Market exposure amount for the 3rd currency in quotation currency of the underlying asset ( C )":
            "29_Market exposure amount for the 3rd currency in quotation currency of the underlying asset (C)",
        "35_Identification type for interest rate index ": 
            "35_Identification type for interest rate index",
        "58b_Nature of the TRANCHE ": 
            "58b_Nature of the TRANCHE",
        "62_Conversion factor (convertibles)/ concordance factor  / parity (options)": 
            "62_Conversion factor (convertibles) / concordance factor / parity (options)",
        "71_Quotation currency of the underlying asset ( C )": 
            "71_Quotation currency of the underlying asset (C)",
        "123_Fund CIC code ":
            "123_Fund CIC code"
        }, inplace=True)

    report.replace({"BIL Invest - ": "BIL Invest "}, regex=True, inplace=True)
    report.replace({"Dynasty Corporate Bonds 0 - 2.5 Class A": "Dynasty Corporate Bonds 0-2.5 A EUR"}, regex=True, inplace=True)
    report.replace({"Dynasty Corporate Bonds 0 - 2.5 Class B": "Dynasty Corporate Bonds 0-2.5 B EUR"}, regex=True, inplace=True)
    report.replace({"Subscription tax IEH": "Subscription tax"}, regex=True, inplace=True)
    report.replace({"Subscription tax I": "Subscription tax"}, regex=True, inplace=True)
    report.replace({"VERSE": "1"}, regex=True, inplace=True)
    report.replace({"RECU": "2"}, regex=True, inplace=True)
    report.replace({"RBC Investor Services Bank S.A.": "RBC Luxembourg"}, regex=True, inplace=True)
    report["125_Accrued Income (Security Denominated Currency)"].fillna(0, inplace=True)
    report["126_Accrued Income (Portfolio Denominated Currency)"].fillna(0, inplace=True)
    #report["59_Credit quality step"].replace({9 : 3}, inplace=True)
    #report["39_Maturity date"] = pd.to_datetime(report["39_Maturity date"]).dt.date
    report["39_Maturity date"] = report["39_Maturity date"].astype('str').replace({"2099-12-31" : "9999-12-31"})
    report["39_Maturity date"].replace("NaT", np.nan, inplace=True)
    report["39_Maturity date"].replace({" 00:00:00": ""}, regex=True, inplace=True)
    report["117_Fund Issuer Name"].replace({"BIL Invest Equities Emerging Markets" : "BIL Invest Equities Emerging Market"},
                                           inplace=True)
    report["49_Name of the group of the issuer"] = report["49_Name of the group of the issuer"].str.strip()
    report["46_Issuer name"].replace({"RBC Luxembourg":"RBC Investor Services Bank S.A."}, regex=True, inplace=True)
    return report

#@pytest.fixture(scope="module")
#def fetcher(params_fixt):    
#    """
#    Instanciate the database fetcher object
#    """
#
#    CLIENT, ISIN, DATE, _ = params_fixt
#    
#    f = TPT_Fetcher(DATE, CLIENT, ISIN, SOURCE_DIR)
#
#    return f

@pytest.fixture(scope="module")
def data_bucket(params_fixt):
    CLIENT, ISIN, DATE, _ = params_fixt
    SOURCE_DIR = Path('./data')
    b = Data_Bucket(DATE, CLIENT, ISIN, SOURCE_DIR)
    b.fetch()

    return b

@pytest.fixture(scope="module")
def generator(params_fixt):
    """
    Instanciate the TPT report generator object
    """

    CLIENT, ISIN, DATE, _ = params_fixt
    SOURCE_DIR = Path('./data')
    OUTPUT_DIR = Path('./TEST_DIR')
    DATE = pd.to_datetime(DATE).date()
    g = TPT_Generator(DATE,
                      CLIENT,
                      ISIN,
                      SOURCE_DIR,
                      OUTPUT_DIR)
    
    return g

def test_bucket_init(data_bucket, params_fixt):
    """
    test initialisation of fetcher object
    """

    CLIENT, ISIN, DATE, _ = params_fixt
    
    assert data_bucket.client == CLIENT
    assert data_bucket.shareclass_isin == ISIN
    assert data_bucket.date == DATE
    
def test_get_shareclass_infos(data_bucket, reference_TPT_report):
    """
    test execution of SQL request for shareclass infos
    """

    shareclass_infos = data_bucket.get_shareclass_infos()
    
    assert shareclass_infos["code_isin"].iloc[0] == \
        reference_TPT_report["1_Portfolio identifying data"].iloc[0]

def test_get_subfund_infos(data_bucket, reference_TPT_report):
    """
    test execution of SQL request for subfund infos
    """

    subfund_infos = data_bucket.get_subfund_infos()

    assert subfund_infos["subfund_lei"].iloc[0] == \
        reference_TPT_report["115_Fund Issuer Code"].iloc[0]

def test_get_fund_infos(data_bucket, reference_TPT_report):
    """
    test execution of SQL request for fund infos
    """

    fund_infos = data_bucket.get_fund_infos()

    assert fund_infos["fund_issuer_group_code"].iloc[0] == reference_TPT_report["119_Fund Issuer Group Code"].iloc[0]

def test_get_shareclass_nav(data_bucket, params_fixt):
    """
    test execution of SQL request for shareclass nav
    """
    _, _, DATE, _ = params_fixt
    shareclass_nav = data_bucket.get_shareclass_nav()

    assert shareclass_nav["nav_date"].iloc[0] == DATE

def test_get_instruments(data_bucket, reference_TPT_report):
    """
    test execution of SQL request for instruments associated to the shareclass
    """
    
    instruments = data_bucket.get_instruments()

    assert len(instruments.index) == len(reference_TPT_report.index)

def test_get_instruments_infos(data_bucket, reference_TPT_report):
    """
    test execution of SQL request for instruments infos
    """
    instruments_infos = data_bucket.get_instruments_infos()
    prod = instruments_infos.sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    #print(ref)
    #diff1 = prod.loc[~(prod.index.isin(ref.index))]
    #print(diff1)
    #diff2 = ref.loc[~(ref.index.isin(prod.index))]
    #print(diff2)
    diff1 = prod.loc[prod["17_Instrument name"] != ref["17_Instrument name"], "17_Instrument name"]
    print(diff1)
    diff2 = ref.loc[ref["17_Instrument name"] != prod["17_Instrument name"], "17_Instrument name"]
    print(diff2)
    #print(prod.index)
    #print(ref.index)

    assert_series_equal(prod["17_Instrument name"],
                        ref["17_Instrument name"])

def test_create_empty_report(generator, reference_TPT_report):
    
    generator.create_empty_report()
    #print(reference_TPT_report.info)
    assert generator.TPT_report.shape == reference_TPT_report.shape
    assert_index_equal(generator.TPT_report.columns, reference_TPT_report.columns)

def test_fill_column_1(generator, reference_TPT_report):

    generator.fill_column_1()
    column = generator.TPT_report["1_Portfolio identifying data"]

    assert_series_equal(column, reference_TPT_report["1_Portfolio identifying data"])

def test_fill_column_2(generator, reference_TPT_report):

    generator.fill_column_2()
    column = generator.TPT_report["2_Type of identification code for the fund share or portfolio"]

    assert_series_equal(column, reference_TPT_report["2_Type of identification code for the fund share or portfolio"])

def test_fill_column_3(generator, reference_TPT_report):

    generator.fill_column_3()
    column = generator.TPT_report["3_Portfolio name"]
    assert_series_equal(column, reference_TPT_report["3_Portfolio name"])

def test_fill_column_4(generator, reference_TPT_report):

    generator.fill_column_4()

    column = generator.TPT_report["4_Portfolio currency (B)"]
    assert_series_equal(column, reference_TPT_report["4_Portfolio currency (B)"])

def test_fill_column_5(generator, reference_TPT_report):

    generator.fill_column_5()

    column = generator.TPT_report["5_Net asset valuation of the portfolio or the share class in portfolio currency"]
    assert_series_equal(column, reference_TPT_report["5_Net asset valuation of the portfolio or the share class in portfolio currency"])

def test_fill_column_6(generator, reference_TPT_report):

    generator.fill_column_6()

    column = generator.TPT_report["6_Valuation date"]
    assert_series_equal(column, reference_TPT_report["6_Valuation date"])

def test_fill_column_7(generator, reference_TPT_report):

    generator.fill_column_7()

    column = generator.TPT_report["7_Reporting date"]
    assert_series_equal(column, reference_TPT_report["7_Reporting date"])

def test_fill_column_8(generator, reference_TPT_report):

    generator.fill_column_8()

    column = generator.TPT_report["8_Share price"]
    assert_series_equal(column, reference_TPT_report["8_Share price"])

def test_fill_column_8b(generator, reference_TPT_report):

    generator.fill_column_8b()

    column = generator.TPT_report["8b_Total number of shares"]
    assert_series_equal(column, reference_TPT_report["8b_Total number of shares"])

def test_fill_column_9(generator, reference_TPT_report):

    generator.fill_column_9()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod[r"9_% cash"]).round(5)
    ref_col = pd.to_numeric(ref[r"9_% cash"]).round(5)

    diff1 = prod.loc[~(prod.index.isin(ref.index))]
    print(diff1)
    diff2 = ref.loc[~(ref.index.isin(prod.index))]
    print(diff2)
    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, check_dtype=False)

def test_fill_column_10(generator, reference_TPT_report):

    generator.fill_column_10()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod["10_Portfolio Modified Duration"].loc[prod["10_Portfolio Modified Duration"] != ref["10_Portfolio Modified Duration"]]
    print(diff1)
    diff2 = ref["10_Portfolio Modified Duration"].loc[ref["10_Portfolio Modified Duration"] != prod["10_Portfolio Modified Duration"]]
    print(diff2)

    column = prod["10_Portfolio Modified Duration"]
    assert_series_equal(column, ref["10_Portfolio Modified Duration"], check_dtype=False)

def test_fill_column_11(generator, reference_TPT_report):

    generator.fill_column_14()
    generator.fill_column_11()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["11_Complete SCR Delivery"]
    assert_series_equal(column, ref["11_Complete SCR Delivery"], check_dtype=False)

def test_fill_column_12(generator, reference_TPT_report):

    generator.fill_column_12()

    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["12_CIC code of the instrument"]

    assert_series_equal(column, ref["12_CIC code of the instrument"], check_dtype=False)

def test_fill_column_13(generator, reference_TPT_report):

    generator.fill_column_13()

    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["13_Economic zone of the quotation place"]

    assert_series_equal(column, ref["13_Economic zone of the quotation place"], check_dtype=False)

def test_fill_column_14(generator, reference_TPT_report):

    generator.fill_column_14()

    column = generator.TPT_report["14_Identification code of the financial instrument"].sort_values()
    column = column.reset_index(drop=True)
    ref = reference_TPT_report["14_Identification code of the financial instrument"].sort_values()
    ref = ref.reset_index(drop=True)
    assert_series_equal(column, ref)

def test_fill_column_15(generator, reference_TPT_report):

    generator.fill_column_15()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["15_Type of identification code for the instrument"]
    assert_series_equal(column, ref["15_Type of identification code for the instrument"], check_dtype=False)

def test_fill_column_16(generator, reference_TPT_report):

    generator.fill_column_16()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["16_Grouping code for multiple leg instruments"]

    assert_series_equal(column, ref["16_Grouping code for multiple leg instruments"], check_dtype=False)

def test_fill_column_17(generator, reference_TPT_report):

    generator.fill_column_17()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["17_Instrument name"]
    assert_series_equal(column, ref["17_Instrument name"])

def test_fill_column_18(generator, reference_TPT_report):

    generator.fill_column_18()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["18_Quantity"]).round(5)
    ref_col = pd.to_numeric(ref["18_Quantity"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_exact=False)

def test_fill_column_19(generator, reference_TPT_report):

    generator.fill_column_19()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["19_Nominal amount"]).round(5)
    ref_col = pd.to_numeric(ref["19_Nominal amount"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_20(generator, reference_TPT_report):

    generator.fill_column_20()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    diff1 = prod["20_Contract size for derivatives"].loc[prod["20_Contract size for derivatives"] != ref["20_Contract size for derivatives"]]
    print(diff1)
    diff2 = ref["20_Contract size for derivatives"].loc[ref["20_Contract size for derivatives"] != prod["20_Contract size for derivatives"]]
    print(diff2)

    column = prod["20_Contract size for derivatives"]
    assert_series_equal(column, ref["20_Contract size for derivatives"], check_dtype=False)

def test_fill_column_21(generator, reference_TPT_report):

    generator.fill_column_21()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    diff1 = prod["21_Quotation currency (A)"].loc[prod["21_Quotation currency (A)"] != ref["21_Quotation currency (A)"]]
    print(diff1)
    diff2 = ref["21_Quotation currency (A)"].loc[ref["21_Quotation currency (A)"] != prod["21_Quotation currency (A)"]]
    print(diff2)

    column = prod["21_Quotation currency (A)"]
    assert_series_equal(column, ref["21_Quotation currency (A)"],
                        check_dtype=False)

def test_fill_column_22(generator, reference_TPT_report):

    generator.fill_column_22()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["22_Market valuation in quotation currency (A)"]).round(5)
    ref_col = pd.to_numeric(ref["22_Market valuation in quotation currency (A)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, check_less_precise=2, check_dtype=False)

def test_fill_column_23(generator, reference_TPT_report):

    generator.fill_column_23()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["23_Clean market valuation in quotation currency (A)"]).round(5)
    ref_col = pd.to_numeric(ref["23_Clean market valuation in quotation currency (A)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_24(generator, reference_TPT_report):

    generator.fill_column_24()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["24_Market valuation in portfolio currency (B)"]).round(5)
    ref_col = pd.to_numeric(ref["24_Market valuation in portfolio currency (B)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, check_dtype=False)

def test_fill_column_25(generator, reference_TPT_report):

    generator.fill_column_25()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["25_Clean market valuation in portfolio currency (B)"]).round(5)
    ref_col = pd.to_numeric(ref["25_Clean market valuation in portfolio currency (B)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,)

def test_fill_column_26(generator, reference_TPT_report):

    generator.fill_column_26()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["26_Valuation weight"]).round(5)
    ref_col = pd.to_numeric(ref["26_Valuation weight"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_less_precise=5)

def test_fill_column_27(generator, reference_TPT_report):

    generator.fill_column_27()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["27_Market exposure amount in quotation currency (A)"]).round(5)
    ref_col = pd.to_numeric(ref["27_Market exposure amount in quotation currency (A)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_28(generator, reference_TPT_report):

    generator.fill_column_28()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["28_Market exposure amount in portfolio currency (B)"]).round(5)
    ref_col = pd.to_numeric(ref["28_Market exposure amount in portfolio currency (B)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_29(generator, reference_TPT_report):

    generator.fill_column_28()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["29_Market exposure amount for the 3rd currency in quotation currency of the underlying asset (C)"]
    assert_series_equal(column, ref["29_Market exposure amount for the 3rd currency in quotation currency of the underlying asset (C)"], check_dtype=False)

def test_fill_column_30(generator, reference_TPT_report):

    generator.fill_column_30()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["30_Market Exposure in weight"]).round(7)
    ref_col = pd.to_numeric(ref["30_Market Exposure in weight"]).round(7)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_31(generator, reference_TPT_report):

    #generator.fill_column_31()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["31_Market exposure for the 3rd currency in weight over NAV"]
    assert_series_equal(column, ref["31_Market exposure for the 3rd currency in weight over NAV"], check_dtype=False)

def test_fill_column_32(generator, reference_TPT_report):

    generator.fill_column_32()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["32_Interest rate type"]
    assert_series_equal(column, ref["32_Interest rate type"], check_dtype=False)

def test_fill_column_33(generator, reference_TPT_report):

    generator.fill_column_33()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["33_Coupon rate"]
    assert_series_equal(column, ref["33_Coupon rate"], check_dtype=False)

def test_fill_column_34(generator, reference_TPT_report):

    generator.fill_column_34()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["34_Interest rate reference identification"]
    assert_series_equal(column, ref["34_Interest rate reference identification"], check_dtype=False)

def test_fill_column_35(generator, reference_TPT_report):

    generator.fill_column_35()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["35_Identification type for interest rate index"]
    assert_series_equal(column, ref["35_Identification type for interest rate index"], check_dtype=False)

def test_fill_column_36(generator, reference_TPT_report):

    generator.fill_column_36()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["36_Interest rate index name"]
    assert_series_equal(column, ref["36_Interest rate index name"], check_dtype=False)

def test_fill_column_37(generator, reference_TPT_report):

    generator.fill_column_37()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["37_Interest rate Margin"]
    assert_series_equal(column, ref["37_Interest rate Margin"], check_dtype=False)

def test_fill_column_38(generator, reference_TPT_report):

    generator.fill_column_38()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["38_Coupon payment frequency"]
    assert_series_equal(column, ref["38_Coupon payment frequency"], check_dtype=False)

def test_fill_column_39(generator, reference_TPT_report):

    generator.fill_column_39()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    ref = ref["39_Maturity date"]
    prod = prod["39_Maturity date"]
    diff1 = prod.loc[prod.astype('str') != ref.astype('str')]
    print(diff1)
    diff2 = ref.loc[ref.astype('str') != prod.astype('str')]

    print(diff2)

    assert_series_equal(prod.astype('str'), ref.astype('str'), check_dtype=False)

def test_fill_column_40(generator, reference_TPT_report):

    generator.fill_column_40()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["40_Redemption type"]
    assert_series_equal(column, ref["40_Redemption type"], check_dtype=False)

def test_fill_column_41(generator, reference_TPT_report):

    generator.fill_column_41()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["41_Redemption rate"]
    assert_series_equal(column, ref["41_Redemption rate"], check_dtype=False)

def test_fill_column_42(generator, reference_TPT_report):

    generator.fill_column_42()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["42_Callable / putable"]
    assert_series_equal(column, ref["42_Callable / putable"], check_dtype=False)

def test_fill_column_43(generator, reference_TPT_report):

    generator.fill_column_43()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["43_Call / put date"]
    assert_series_equal(column, ref["43_Call / put date"], check_dtype=False)

def test_fill_column_44(generator, reference_TPT_report):

    generator.fill_column_44()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["44_Issuer / bearer option exercise"]
    assert_series_equal(column, ref["44_Issuer / bearer option exercise"], check_dtype=False)

def test_fill_column_45(generator, reference_TPT_report):

    generator.fill_column_45()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["45_Strike price for embedded (call/put) options"]
    assert_series_equal(column, ref["45_Strike price for embedded (call/put) options"], check_dtype=False)

def test_fill_column_46(generator, reference_TPT_report):

    generator.fill_column_46()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    prod["46_Issuer name"].replace({"GroupamaEtat" : "Groupama Etat"}, regex=True, inplace=True)
    ref["46_Issuer name"].replace({"Groupama  Etat" : "Groupama Etat"}, regex=True, inplace=True)

    diff1 = prod["46_Issuer name"].loc[prod["46_Issuer name"] != ref["46_Issuer name"]]
    print(diff1)
    diff2 = ref["46_Issuer name"].loc[ref["46_Issuer name"] != prod["46_Issuer name"]]
    print(diff2)

    column = prod["46_Issuer name"]
    assert_series_equal(column, ref["46_Issuer name"].str.rstrip(),)

def test_fill_column_47(generator, reference_TPT_report):

    generator.fill_column_47()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["47_Issuer identification code"]
    assert_series_equal(column, ref["47_Issuer identification code"], check_dtype=False)

def test_fill_column_48(generator, reference_TPT_report):

    generator.fill_column_48()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["48_Type of identification code for issuer"]
    assert_series_equal(column, ref["48_Type of identification code for issuer"], check_dtype=False)

def test_fill_column_49(generator, reference_TPT_report):

    generator.fill_column_49()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod["49_Name of the group of the issuer"].loc[prod["49_Name of the group of the issuer"] != ref["49_Name of the group of the issuer"]]
    print(diff1)
    diff2 = ref["49_Name of the group of the issuer"].loc[ref["49_Name of the group of the issuer"] != prod["49_Name of the group of the issuer"]]
    print(diff2)

    column = prod["49_Name of the group of the issuer"]
    assert_series_equal(column, ref["49_Name of the group of the issuer"], check_dtype=False)

def test_fill_column_50(generator, reference_TPT_report):

    generator.fill_column_50()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod["50_Identification of the group"].loc[prod["50_Identification of the group"] != ref["50_Identification of the group"]]
    print(diff1)
    diff2 = ref["50_Identification of the group"].loc[ref["50_Identification of the group"] != prod["50_Identification of the group"]]
    print(diff2)

    column = prod["50_Identification of the group"]
    assert_series_equal(column, ref["50_Identification of the group"], check_dtype=False)

def test_fill_column_51(generator, reference_TPT_report):

    generator.fill_column_51()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["51_Type of identification code for issuer group"]
    assert_series_equal(column, ref["51_Type of identification code for issuer group"], check_dtype=False)

def test_fill_column_52(generator, reference_TPT_report):

    generator.fill_column_52()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["52_Issuer country"]
    assert_series_equal(column, ref["52_Issuer country"], check_dtype=False)

def test_fill_column_53(generator, reference_TPT_report):

    generator.fill_column_53()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["53_Issuer economic area"]
    assert_series_equal(column, ref["53_Issuer economic area"], check_dtype=False)

def test_fill_column_54(generator, reference_TPT_report):

    generator.fill_column_54()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["54_Economic sector"]
    assert_series_equal(column, ref["54_Economic sector"], check_dtype=False)

def test_fill_column_55(generator, reference_TPT_report):

    generator.fill_column_55()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["55_Covered / not covered"]
    assert_series_equal(column, ref["55_Covered / not covered"], check_dtype=False)

def test_fill_column_56(generator, reference_TPT_report):

    generator.fill_column_56()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["56_Securitisation"]
    assert_series_equal(column, ref["56_Securitisation"], check_dtype=False)

def test_fill_column_57(generator, reference_TPT_report):

    generator.fill_column_57()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["57_Explicit guarantee by the country of issue"]
    assert_series_equal(column, ref["57_Explicit guarantee by the country of issue"], check_dtype=False)

def test_fill_column_58(generator, reference_TPT_report):

    generator.fill_column_58()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["58_Subordinated debt"]
    assert_series_equal(column, ref["58_Subordinated debt"], check_dtype=False)

def test_fill_column_58b(generator, reference_TPT_report):

    generator.fill_column_58b()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref.replace("xxxdeletexxx", "", inplace=True)
    column = prod["58b_Nature of the TRANCHE"]
    assert_series_equal(column, ref["58b_Nature of the TRANCHE"], check_dtype=False)

def test_fill_column_59(generator, reference_TPT_report):
    generator.fill_column_59()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod.loc[prod["59_Credit quality step"] != ref["59_Credit quality step"]]
    print(diff1.loc[:, ["12_CIC code of the instrument", "59_Credit quality step"]])
    diff2 = ref.loc[ref["59_Credit quality step"] != prod["59_Credit quality step"]]
    print(diff2.loc[:, ["12_CIC code of the instrument", "59_Credit quality step"]])

    column = prod["59_Credit quality step"]
    assert_series_equal(column, ref["59_Credit quality step"], check_dtype=False)

def test_fill_column_60(generator, reference_TPT_report):

    generator.fill_column_60()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["60_Call / Put / Cap / Floor"]
    assert_series_equal(column, ref["60_Call / Put / Cap / Floor"], check_dtype=False)

def test_fill_column_61(generator, reference_TPT_report):

    generator.fill_column_61()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["61_Strike price"]
    assert_series_equal(column, ref["61_Strike price"], check_dtype=False)

def test_fill_column_62(generator, reference_TPT_report):

    generator.fill_column_62()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["62_Conversion factor (convertibles) / concordance factor / parity (options)"]).round(5)
    ref_col = pd.to_numeric(ref["62_Conversion factor (convertibles) / concordance factor / parity (options)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_exact=False)

def test_fill_column_63(generator, reference_TPT_report):

    generator.fill_column_63()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["63_Effective Date of Instrument"]
    assert_series_equal(column, ref["63_Effective Date of Instrument"], check_dtype=False)

def test_fill_column_64(generator, reference_TPT_report):

    generator.fill_column_64()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["64_Exercise type"]
    assert_series_equal(column, ref["64_Exercise type"], check_dtype=False)

def test_fill_column_65(generator, reference_TPT_report):

    generator.fill_column_65()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["65_Hedging Rolling"]
    assert_series_equal(column, ref["65_Hedging Rolling"], check_dtype=False)

#def test_fill_column_66(generator, reference_TPT_report):
#
#    generator.fill_column_62()
#    
#    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
#    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
#
#    column = prod["62_Conversion factor (convertibles) / concordance factor / parity (options)"]
#    assert_series_equal(column, ref["62_Conversion factor (convertibles) / concordance factor / parity (options)"], check_dtype=False)

def test_fill_column_67(generator, reference_TPT_report):

    generator.fill_column_67()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["67_CIC code of the underlying asset"]
    assert_series_equal(column, ref["67_CIC code of the underlying asset"], check_dtype=False)

def test_fill_column_68(generator, reference_TPT_report):

    generator.fill_column_68()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["68_Identification code of the underlying asset"]
    assert_series_equal(column, ref["68_Identification code of the underlying asset"], check_dtype=False)

def test_fill_column_69(generator, reference_TPT_report):

    generator.fill_column_69()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["69_Type of identification code for the underlying asset"]
    assert_series_equal(column, ref["69_Type of identification code for the underlying asset"], check_dtype=False)

def test_fill_column_70(generator, reference_TPT_report):

    generator.fill_column_70()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod["70_Name of the underlying asset"].loc[prod["70_Name of the underlying asset"] != ref["70_Name of the underlying asset"]]
    print(diff1)
    diff2 = ref["70_Name of the underlying asset"].loc[ref["70_Name of the underlying asset"] != prod["70_Name of the underlying asset"]]
    print(diff2)
    
    column = prod["70_Name of the underlying asset"]
    assert_series_equal(column, ref["70_Name of the underlying asset"], check_dtype=False)

def test_fill_column_71(generator, reference_TPT_report):

    generator.fill_column_71()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    diff1 = prod["71_Quotation currency of the underlying asset (C)"].loc[prod["71_Quotation currency of the underlying asset (C)"] != ref["71_Quotation currency of the underlying asset (C)"]]
    print(diff1)
    diff2 = ref["71_Quotation currency of the underlying asset (C)"].loc[ref["71_Quotation currency of the underlying asset (C)"] != prod["71_Quotation currency of the underlying asset (C)"]]
    print(diff2)
    
    column = prod["71_Quotation currency of the underlying asset (C)"]
    assert_series_equal(column, ref["71_Quotation currency of the underlying asset (C)"], check_dtype=False)

def test_fill_column_72(generator, reference_TPT_report):

    generator.fill_column_72()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["72_Last valuation price of the underlying asset"]
    assert_series_equal(column, ref["72_Last valuation price of the underlying asset"], check_dtype=False)

def test_fill_column_73(generator, reference_TPT_report):

    generator.fill_column_73()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["73_Country of quotation of the underlying asset"]
    assert_series_equal(column, ref["73_Country of quotation of the underlying asset"], check_dtype=False)

def test_fill_column_74(generator, reference_TPT_report):

    generator.fill_column_74()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["74_Economic Area of quotation of the underlying asset"]
    assert_series_equal(column, ref["74_Economic Area of quotation of the underlying asset"], check_dtype=False)

def test_fill_column_75(generator, reference_TPT_report):

    generator.fill_column_75()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["75_Coupon rate of the underlying asset"]
    assert_series_equal(column, ref["75_Coupon rate of the underlying asset"], check_dtype=False)

def test_fill_column_76(generator, reference_TPT_report):

    generator.fill_column_76()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["76_Coupon payment frequency of the underlying asset"]
    assert_series_equal(column, ref["76_Coupon payment frequency of the underlying asset"], check_dtype=False)

def test_fill_column_77(generator, reference_TPT_report):

    generator.fill_column_77()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["77_Maturity date of the underlying asset"]
    assert_series_equal(column, ref["77_Maturity date of the underlying asset"], check_dtype=False)

def test_fill_column_78(generator, reference_TPT_report):

    generator.fill_column_78()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["78_Redemption profile of the underlying asset"]
    assert_series_equal(column, ref["78_Redemption profile of the underlying asset"], check_dtype=False)

def test_fill_column_79(generator, reference_TPT_report):

    generator.fill_column_79()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["79_Redemption rate of the underlying asset"]
    assert_series_equal(column, ref["79_Redemption rate of the underlying asset"], check_dtype=False)

def test_fill_column_80(generator, reference_TPT_report):

    generator.fill_column_80()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["80_Issuer name of the underlying asset"]
    assert_series_equal(column, ref["80_Issuer name of the underlying asset"], check_dtype=False)

def test_fill_column_81(generator, reference_TPT_report):

    generator.fill_column_81()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["81_Issuer identification code of the underlying asset"]
    assert_series_equal(column, ref["81_Issuer identification code of the underlying asset"], check_dtype=False)

def test_fill_column_82(generator, reference_TPT_report):

    generator.fill_column_82()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["82_Type of issuer identification code of the underlying asset"]
    assert_series_equal(column, ref["82_Type of issuer identification code of the underlying asset"], check_dtype=False)

def test_fill_column_83(generator, reference_TPT_report):

    generator.fill_column_83()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["83_Name of the group of the issuer of the underlying asset"]
    assert_series_equal(column, ref["83_Name of the group of the issuer of the underlying asset"], check_dtype=False)

def test_fill_column_84(generator, reference_TPT_report):

    generator.fill_column_84()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["84_Identification of the group of the underlying asset"]
    assert_series_equal(column, ref["84_Identification of the group of the underlying asset"], check_dtype=False)

def test_fill_column_85(generator, reference_TPT_report):

    generator.fill_column_85()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["85_Type of the group identification code of the underlying asset"]
    assert_series_equal(column, ref["85_Type of the group identification code of the underlying asset"], check_dtype=False)

def test_fill_column_86(generator, reference_TPT_report):

    generator.fill_column_86()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["86_Issuer country of the underlying asset"]
    assert_series_equal(column, ref["86_Issuer country of the underlying asset"], check_dtype=False)

def test_fill_column_87(generator, reference_TPT_report):

    generator.fill_column_87()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["87_Issuer economic area of the underlying asset"]
    assert_series_equal(column, ref["87_Issuer economic area of the underlying asset"], check_dtype=False)

def test_fill_column_88(generator, reference_TPT_report):

    generator.fill_column_88()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["88_Explicit guarantee by the country of issue of the underlying asset"]
    assert_series_equal(column, ref["88_Explicit guarantee by the country of issue of the underlying asset"], check_dtype=False)

def test_fill_column_89(generator, reference_TPT_report):

    generator.fill_column_89()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["89_Credit quality step of the underlying asset"]
    assert_series_equal(column, ref["89_Credit quality step of the underlying asset"], check_dtype=False)

def test_fill_column_90(generator, reference_TPT_report):

    generator.fill_column_90()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["90_Modified Duration to maturity date"]
    assert_series_equal(column, ref["90_Modified Duration to maturity date"], check_dtype=False)

def test_fill_column_91(generator, reference_TPT_report):

    generator.fill_column_91()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["91_Modified duration to next option exercise date"]
    assert_series_equal(column, ref["91_Modified duration to next option exercise date"], check_dtype=False)

def test_fill_column_92(generator, reference_TPT_report):

    generator.fill_column_92()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["92_Credit sensitivity"]
    assert_series_equal(column, ref["92_Credit sensitivity"], check_dtype=False)

def test_fill_column_93(generator, reference_TPT_report):

    generator.fill_column_93()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["93_Sensitivity to underlying asset price (delta)"]
    assert_series_equal(column, ref["93_Sensitivity to underlying asset price (delta)"], check_dtype=False)

def test_fill_column_94(generator, reference_TPT_report):

    generator.fill_column_94()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["94_Convexity / gamma for derivatives"]
    assert_series_equal(column, ref["94_Convexity / gamma for derivatives"], check_dtype=False)

def test_fill_column_94b(generator, reference_TPT_report):

    generator.fill_column_94b()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["94b_Vega"]
    assert_series_equal(column, ref["94b_Vega"], check_dtype=False)

def test_fill_column_95(generator, reference_TPT_report):

    #generator.fill_column_95()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["95_Identification of the original portfolio for positions embedded in a fund"]
    assert_series_equal(column, ref["95_Identification of the original portfolio for positions embedded in a fund"], check_dtype=False)

def test_fill_column_97(generator, reference_TPT_report):

    generator.fill_column_97()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["97_SCR_Mrkt_IR_up weight over NAV"]).round(5)
    ref_col = pd.to_numeric(ref["97_SCR_Mrkt_IR_up weight over NAV"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_98(generator, reference_TPT_report):

    generator.fill_column_98()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["98_SCR_Mrkt_IR_down weight over NAV"]).round(5)
    ref_col = pd.to_numeric(ref["98_SCR_Mrkt_IR_down weight over NAV"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False,
                        check_less_precise=2)

def test_fill_column_99(generator, reference_TPT_report):

    generator.fill_column_99()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["99_SCR_Mrkt_Eq_type1 weight over NAV"]).round(5)
    ref_col = pd.to_numeric(ref["99_SCR_Mrkt_Eq_type1 weight over NAV"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False)

def test_fill_column_100(generator, reference_TPT_report):

    generator.fill_column_100()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["100_SCR_Mrkt_Eq_type2 weight over NAV"]).round(12)
    ref_col = pd.to_numeric(ref["100_SCR_Mrkt_Eq_type2 weight over NAV"]).round(12)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False)

def test_fill_column_101(generator, reference_TPT_report):

    #generator.fill_column_101()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["101_SCR_Mrkt_Prop weight over NAV"]
    assert_series_equal(column, ref["101_SCR_Mrkt_Prop weight over NAV"], check_dtype=False)

def test_fill_column_102(generator, reference_TPT_report):

    generator.fill_column_102()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["102_SCR_Mrkt_Spread_bonds weight over NAV"]).round(12)
    ref_col = pd.to_numeric(ref["102_SCR_Mrkt_Spread_bonds weight over NAV"]).round(12)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False)

def test_fill_column_103(generator, reference_TPT_report):

    #generator.fill_column_103()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["103_SCR_Mrkt_Spread_structured weight over NAV"]
    assert_series_equal(column, ref["103_SCR_Mrkt_Spread_structured weight over NAV"], check_dtype=False)

def test_fill_column_104(generator, reference_TPT_report):

    #generator.fill_column_104()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["104_SCR_Mrkt_Spread_derivatives_up weight over NAV"]
    assert_series_equal(column, ref["104_SCR_Mrkt_Spread_derivatives_up weight over NAV"], check_dtype=False)

def test_fill_column_105(generator, reference_TPT_report):

    #generator.fill_column_105()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["105_SCR_Mrkt_Spread_derivatives_down weight over NAV"]
    assert_series_equal(column, ref["105_SCR_Mrkt_Spread_derivatives_down weight over NAV"], check_dtype=False)

def test_fill_column_105a(generator, reference_TPT_report):

    generator.fill_column_105a()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["105a_SCR_Mrkt_FX_up weight over NAV"]).round(12)
    ref_col = pd.to_numeric(ref["105a_SCR_Mrkt_FX_up weight over NAV"]).round(12)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False)

def test_fill_column_105b(generator, reference_TPT_report):

    generator.fill_column_105b()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    column = pd.to_numeric(prod["105b_SCR_Mrkt_FX_down weight over NAV"]).round(12)
    ref_col = pd.to_numeric(ref["105b_SCR_Mrkt_FX_down weight over NAV"]).round(12)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col,
                        check_dtype=False)

def test_fill_column_106(generator, reference_TPT_report):

    #generator.fill_column_106()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["106_Asset pledged as collateral"]
    assert_series_equal(column, ref["106_Asset pledged as collateral"], check_dtype=False)

def test_fill_column_107(generator, reference_TPT_report):

    #generator.fill_column_107()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["107_Place of deposit"]
    assert_series_equal(column, ref["107_Place of deposit"], check_dtype=False)

def test_fill_column_108(generator, reference_TPT_report):

    #generator.fill_column_108()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["108_Participation"]
    assert_series_equal(column, ref["108_Participation"], check_dtype=False)

def test_fill_column_110(generator, reference_TPT_report):

    #generator.fill_column_110()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["110_Valorisation method"]
    assert_series_equal(column, ref["110_Valorisation method"], check_dtype=False)

def test_fill_column_111(generator, reference_TPT_report):

    #generator.fill_column_111()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["111_Value of acquisition"]
    assert_series_equal(column, ref["111_Value of acquisition"], check_dtype=False)

def test_fill_column_112(generator, reference_TPT_report):

    #generator.fill_column_112()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["112_Credit rating"]
    assert_series_equal(column, ref["112_Credit rating"], check_dtype=False)

def test_fill_column_113(generator, reference_TPT_report):

    #generator.fill_column_113()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["113_Rating agency"]
    assert_series_equal(column, ref["113_Rating agency"], check_dtype=False)

def test_fill_column_114(generator, reference_TPT_report):

    generator.fill_column_114()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["114_Issuer economic area"]
    assert_series_equal(column, ref["114_Issuer economic area"], check_dtype=False)

def test_fill_column_115(generator, reference_TPT_report):

    generator.fill_column_115()

    column = generator.TPT_report["115_Fund Issuer Code"]
    assert_series_equal(column, reference_TPT_report["115_Fund Issuer Code"])

def test_fill_column_116(generator, reference_TPT_report):

    generator.fill_column_116()

    column = generator.TPT_report["116_Fund Issuer Code Type"]
    assert_series_equal(column, reference_TPT_report["116_Fund Issuer Code Type"])

def test_fill_column_117(generator, reference_TPT_report):

    generator.fill_column_117()

    column = generator.TPT_report["117_Fund Issuer Name"]
    assert_series_equal(column, reference_TPT_report["117_Fund Issuer Name"])

def test_fill_column_118(generator, reference_TPT_report):

    generator.fill_column_118()

    column = generator.TPT_report["118_Fund Issuer Sector"]
    assert_series_equal(column, reference_TPT_report["118_Fund Issuer Sector"])

def test_fill_column_119(generator, reference_TPT_report):

    generator.fill_column_119()

    column = generator.TPT_report["119_Fund Issuer Group Code"]
    assert_series_equal(column, reference_TPT_report["119_Fund Issuer Group Code"])

def test_fill_column_120(generator, reference_TPT_report):

    generator.fill_column_120()

    column = generator.TPT_report["120_Fund Issuer Group Code Type"]
    assert_series_equal(column, reference_TPT_report["120_Fund Issuer Group Code Type"])

def test_fill_column_121(generator, reference_TPT_report):

    generator.fill_column_121()

    column = generator.TPT_report["121_Fund Issuer Group name"]
    assert_series_equal(column, reference_TPT_report["121_Fund Issuer Group name"])

def test_fill_column_122(generator, reference_TPT_report):

    generator.fill_column_122()

    column = generator.TPT_report["122_Fund Issuer Country"]
    assert_series_equal(column, reference_TPT_report["122_Fund Issuer Country"])

def test_fill_column_123(generator, reference_TPT_report):

    generator.fill_column_123()

    column = generator.TPT_report["123_Fund CIC code"]
    assert_series_equal(column, reference_TPT_report["123_Fund CIC code"])

def test_fill_column_123a(generator, reference_TPT_report):

    generator.fill_column_123a()

    column = generator.TPT_report["123a_Fund Custodian Country"]
    assert_series_equal(column, reference_TPT_report["123a_Fund Custodian Country"])

def test_fill_column_124(generator, reference_TPT_report):

    generator.fill_column_124()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["124_Duration"]
    assert_series_equal(column, ref["124_Duration"], check_dtype=False)

def test_fill_column_125(generator, reference_TPT_report):

    generator.fill_column_125()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["125_Accrued Income (Security Denominated Currency)"]).round(5)
    ref_col = pd.to_numeric(ref["125_Accrued Income (Security Denominated Currency)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_exact=False)

def test_fill_column_126(generator, reference_TPT_report):

    generator.fill_column_126()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = pd.to_numeric(prod["126_Accrued Income (Portfolio Denominated Currency)"]).round(5)
    ref_col = pd.to_numeric(ref["126_Accrued Income (Portfolio Denominated Currency)"]).round(5)

    diff1 = column.loc[column != ref_col]
    print(diff1)
    diff2 = ref_col.loc[ref_col != column]
    print(diff2)

    assert_series_equal(column, ref_col, 
                        check_dtype=False,
                        check_exact=False)

def test_fill_column_127(generator, reference_TPT_report):

    generator.fill_column_127()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["127_Bond Floor (convertible instrument only)"]
    assert_series_equal(column, ref["127_Bond Floor (convertible instrument only)"], check_dtype=False)

def test_fill_column_128(generator, reference_TPT_report):

    generator.fill_column_128()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["128_Option premium (convertible instrument only)"]
    assert_series_equal(column, ref["128_Option premium (convertible instrument only)"], check_dtype=False)

def test_fill_column_129(generator, reference_TPT_report):

    #generator.fill_column_129()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["129_Valuation Yield"]
    assert_series_equal(column, ref["129_Valuation Yield"], check_dtype=False)

def test_fill_column_130(generator, reference_TPT_report):

    #generator.fill_column_130()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["130_Valuation Z-spread"]
    assert_series_equal(column, ref["130_Valuation Z-spread"], check_dtype=False)

def test_fill_column_131(generator, reference_TPT_report):

    generator.fill_column_131()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    
    diff1 = prod["131_Underlying Asset Category"].loc[prod["131_Underlying Asset Category"] != ref["131_Underlying Asset Category"]]
    print(diff1)
    diff2 = ref["131_Underlying Asset Category"].loc[ref["131_Underlying Asset Category"] != prod["131_Underlying Asset Category"]]
    print(diff2)

    column = prod["131_Underlying Asset Category"].astype('str')
    assert_series_equal(column, ref["131_Underlying Asset Category"].astype('str'), check_dtype=False)

def test_fill_column_132(generator, reference_TPT_report):

    #generator.fill_column_132()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["132_Infrastructure_investment"]
    assert_series_equal(column, ref["132_Infrastructure_investment"], check_dtype=False)

def test_fill_column_133(generator, reference_TPT_report):

    generator.fill_column_133()

    column = generator.TPT_report["133_custodian_name"]
    assert_series_equal(column, reference_TPT_report["133_custodian_name"])

def test_fill_column_134(generator, reference_TPT_report):

    #generator.fill_column_134()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["134_type1_private_equity_portfolio_eligibility"]
    assert_series_equal(column, ref["134_type1_private_equity_portfolio_eligibility"], check_dtype=False)

def test_fill_column_135(generator, reference_TPT_report):

    #generator.fill_column_135()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["135_type1_private_equity_issuer_beta"]
    assert_series_equal(column, ref["135_type1_private_equity_issuer_beta"], check_dtype=False)

def test_fill_column_137(generator, reference_TPT_report):

    generator.fill_column_137()
    
    prod = generator.TPT_report.set_index("14_Identification code of the financial instrument").sort_index()
    ref = reference_TPT_report.set_index("14_Identification code of the financial instrument").sort_index()

    column = prod["137_counterparty_sector"]
    assert_series_equal(column, ref["137_counterparty_sector"], check_dtype=False)
