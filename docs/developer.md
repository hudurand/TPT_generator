# Developer guide

In this section are detailed the implementation and development guidelines followed for writing reporting tools. 

## Production cycle

database -> processing -> report
See detais on Wiki

## General principles
- (flexible) Object oriented design
- Domain driven design 
- panda as processing engine
- create a framework to build reporting tools
- clean, pythonic, 
- simple patterns/guidelines: chain of responsibility, separation of concerns, python's mixed patterns (couple data-containers and ops)
- simple inheritance patterns (2levels max: parent-childs)

## Implementation design

AOR is used in CLI through th aor.py file:  

For each reports, two function are available: one to generate a single report from command line and another to generate one or multiple reports from a config file.

Each reporting tool should follow the same architecture:

- A Generator which reads the config and output the report.
- The Generator has a DataBucket to hold and access the data during processing
- The DataBucket has a Fetcher to access the database
- The DataBucket has a processor to manage the processing of the data
- the processor has multiple processing modules
    - all processors should have a miscelaneous module handling all minor processing.
- The processor makes sure that the required data has been acquired and call the processing modules.
- The DataBucket uses the chain of responsibility pattern to call the fetcher on the data it's missing.

## Architecture overview

Here is a modelisation of the data flux during the generation of a report.

![modelisation](Generator_V0.1.svg){: .center}