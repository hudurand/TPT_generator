# Details of TPT solvency II methodology
## Required data

* [ ] Client's Portfolio
* [ ] Client's Net asset valuation at shareclass level (currency of shareclass and subfund)
* [ ] Client's infos (client, fund, subfund, shareclasses)
* [ ] Instruments' infos (AODB/instrument table)
* [ ] actualisation rates
* [ ] symmetric adjustment 
* [ ] date
* [ ] currency rates

## Sections

The TPT_report can be divided into a number of sections which are filled with different type of data:

    - columns that are shareclass specific and will hold the same 
    information in all rows. This include the shareclass' related
    informations, as well as the subfund's, fund's and client's related 
    informations.
        
    - fixed instruments specific columns which will hold informations that
    depends on the instruments intrinsecs properties.

    - varying instruments specific columns which will hold informations that
    depends on the specific investment made by the shareclass.

    - SCR columns which are computed by a dedicated module.

???  "Portfolio characteristics and valuation [1-11]"
    === "Methodology"
        Shareclass related informations

        For funds with an ISIN code, the ISIN code is reported.  
        Otherwise, we will report one of the other recognised codes such as:  

        * a CUSIP  
        * Bloomberg Ticker 
        * Reuters RIC  
        * a code attributed by the undertaking  

    === "Required data"

        | table | fields |
        |---|---|
        |shareclass | code_isin |

    === "Code"
        * [TPTGenerator.fill_column_1](http://127.0.0.1:8000/reference/#TPT_generator_python.TPT_generator.TPTGenerator)



??? "Instrument codification [12-17]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"

??? "Valuation and exposures [17b-31]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"

??? "Instrument characteristics and analytics [32-94b]" 
    ??? "Interest rate instruments characteristics [32-45]"
        === "Methodology"

        === "Required data"

            | table | fields |
            |---|---|

        === "Code"
        
    ??? "Issuer data [46-59]"
        === "Methodology"

        === "Required data"

            | table | fields |
            |---|---|

        === "Code"

    ??? "Additional characteristics for derivatives [60-65]"
        === "Methodology"

        === "Required data"

            | table | fields |
            |---|---|

        === "Code"

    ??? "Derivatives / additional characteristics of the underlying asset [67-89]"
        === "Methodology"

        === "Required data"

            | table | fields |
            |---|---|

        === "Code"

    ??? "Analytics [90-94b]"
        === "Methodology"

        === "Required data"

            | table | fields |
            |---|---|

        === "Code"

??? "Transparency [95]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Indicative contributions to SCR (instrument level) [97-105b]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Additional information instruments [106-114]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Additional information portfolio characteristics [115-126]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Specific data for convertible bonds [127-128]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Specific data in case no yield curve of reference is available [129-131]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "Additional fields / additional information [133-135]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"


??? "additional counterparty information [137]"
    === "Methodology"

    === "Required data"

        | table | fields |
        |---|---|


    === "Code"
