---
title: 'rmsp: A command-line tool to make a geocoded dataset of Russian small and medium-sized enterprises (SMEs) from Federal Tax Service open data'
tags:
  - Python
  - business
  - small and medium-sized enterprises
  - open data
authors:
  - name: Pavel O. Syomin
    orcid: 0000-0002-4015-9206
    affiliation: 1
affiliations:
 - name: Independent researcher, Russia
   index: 1
date: 28 March 2025
bibliography: paper.bib
---

# Summary

Federal Tax Service (FTS) of Russia operates a few open datasets with information about all Russian small and medium-sized enterprises (SMEs) that constitute a substantial proportion of all businesses in Russia. This paper introduces a Python command-line tool that automatically extracts and transforms FTS open data to make a geocoded tabular panel dataset about SMEs in Russia. It is aimed to simplify data retrieval for researchers so that they can focus on their study without the need for complex data retrieval and preprocessing.

# Statement of need

Researchers interested in various aspects of Russian economy sometimes face the need to work with disggregated data about particular ogranisations or sole traders. Such data may include, for example, their addresses of incorporation, firm names, revenue, expenditure, average count of employees. Usually a researcher have to use commercial business intelligence services (e.g. Kontur.Focus, Spark Interfax, Ofdata) and pay money to obtain such information. As an alternative, one may refer to a few official open data resources to retrieve the required data. One of the most important of these data resources is open data page of Federal Tax Service website that contains, in particular, the three following datasets: the registry of small and medium-sized enterprises, the information about revenue and expenditure of ogranisations, the information about average number of employees. Unfortunately, these datasets are large and of a complex structure, thus only a minority of reserachers can easily preprocess them to use in their analysis.

The purpose of this tool is to help researchers work with aforementioned FTS open datasets. Now, instead of having to build their own pipeline for data extraction and transformation, they have a ready-to-use tool that generates a tabular dataset in well-known and widely supported CSV format that can be directly analyzed. The tool builds a yearly panel table from a collection of monthly or yearly dumps of open data, carefully removing duplicates and preserving important changes, extracts features of high value dropping redundant ones, normalises address information. In addition, it geocodes human-readible addresses so that each company or sole trader has geographical coordinates detailed down to the level of a particular settlement, thus transforming the source data into geodata and enabling its use for spatial research.

It should be noted that small and medum-sized enterprises are just a subset of all organisations and sole traders. In Russia, a business must have a revenue less than 2B roubles and average number of employees less than 250 to be considered an SME. There are also some additional qualifications for SME related to the absence of government/non-SME control. Thus, the resulting dataset is just a portion of all businesses rather than the entire population. In some economic groups the overwhelming majority of businesses are SMEs, so one can possibly rely just on SMEs to make reasonable conslusions. However, for other economic groups it is not true. Consequently, it is up to researcher to check the completeness and validity of the data produced by this application. In the scenarios when the completeness of data is crucial, one should better use Russian Financial Statements Database (RSFD) that was designed to minimise the data loss. A potential use case for `rmsp` tool is its integration into other data processing pipelines where information about SMEs is necessary yet insufficient.

# Pipeline

The proposed tool implements a five-stage data processing pipeline. I suggest that a Python developer can understand it easily from the code, but still provide a brief description of each stage below.

## Download

FTS open datasets are versioned. The first release of SME registry data was published in 2016 and has been updated monthly, thus the total number of source data files is relatively high. To simplify data loading, a Download stage was included in the tool. It accesses FTS website, parses it to get file links and download files storing them either locally or on Yandex Disk storage. 

## Extract

Source data files are zip archives of xml files. This format is quite unconvenient for data analysis, especially given the volume of data. Extract stage extracts valueable information from zip archives and stores it in csv files, each representing a source archive.

## Aggregate

Regular full updates of open datasets result in a high level of data duplication. If attributes of a business remained untouched between data releases, the two corresponding data records would be completely the same. Aggregate stage removes duplicates and stores time dimension of data in a more compact `start/end date` format.

## Geocode

Source data contains human-readible addresses of SMEs but lacks geographical coordinates. In addition, addresses may have various spellings making it hard to work with the data as spatial data. Geocode stage normalises addresses and geocodes them adding geographical coordinates and municipal codes.

## Panelize

Researchers usually work with flat panel tables storing all attributes of a particular business in a particular year as a single row of a single table. Panelize stage joins data tables and transform the result into a panel dataset with the most convenient structure.

# Discussion and Future Directions

The current situation of open data in Russia raises serious concerns, and I am personally not sure whether the open datasets are going to remain open in the future. However, if they are, there are some possibilities to improve the application, e. g. by increasing the quality of geocoding or by adding more attributes. It is also possible to incorporate some other datasets to the pipeline. I appreciate any help in the development of the app.

I also understand that probably it would be more convenient for some researchers to have the prepared datasets instead of the app, so there is a chance that I will find a way of publishing both the tool and the generated dataset with regular data updates.

# Acknowledgements

I want to thank Dmitry Skougarevsky and Ruslan Kuchakov from Institute for the Rule of Law for their valuable feedback on the necessity, value and potential caveats of working with FTS open data. I also thank Yulia Kuzevanova (ex-coordinator of Research Data Infrastructure — RDI) for the help in publication of some portions of the data generated using this app, as well as numerous members of RDI public chat for expressing their interest in the facilitating access to FTS open data about Russian SMEs that motivated me to finish the development of this tool.

# References