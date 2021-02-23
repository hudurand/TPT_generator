from TPT_generator import TPT_Generator
from timeit import default_timer as timer

if __name__ == "__main__":
    DATE = "2020-12-31"
    CLIENT = "BIL"
    #ISIN = #"LU1689729546"
    ISIN = "LU1689732417"
           #"LU1689729546"
           #"LU1689729629"
           #"LU1808854803"
           #"LU1689730122"
           #"LU1689730718"
           #"LU1689730809"
    start = timer()
    g = TPT_Generator(DATE,
                      CLIENT,
                      ISIN)

    #print(g.cash_flows)
    g.create_empty_report()
    g.generate()
    g.output_excel()
    #print(g.TPT_report.loc[g.TPT_report[g.fields["14"]]=="FR0013245867", g.fields["97"]])
    end = timer()
    print(end - start)