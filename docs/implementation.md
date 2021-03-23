# Implementation details of the TPT generation tool

## Generation steps
- get data from db
    - client infos (cleint, fund, subfund, shareclass)
    - portfolio infos
    - instruments infos
- process instruments 1 (common processing)
- compute repartition
- process instruments 2 (shareclass specific)
- compute SCR
    - compute cash flow
    - compute instruments contribution
    - aggregate SCR

## Modelisation
![solvencyII](TPT_generator_V1.1.svg){: .center}