# Real Estate scraper
Real Estate scraper is a program that scrapes ads from [KOMO](https://www.komo.co.il) and builds an SQL database with detailed information on each ad. Cool!

## Usage
Preparation:
- Download realestatescraper.py and requirements.txt from https://github.com/yosefmentzer/data-mining-realestate.
- Make sure that all required libraries in the requirements.txt file are installed in your system.

From the terminal:
```bash
python realestatescraper.py -p 1 -a 2 -c "Tel Aviv Yaffo"
```
Optional parameters:
-p: property type. 1 for regular apartment, 2 for two-family building, etc. See full list of property types in the help menu:
```bash
python realestatescraper.py --help
```
-a: advertisement type. 1 for sale, 2 for rent.
-c: city name in English. In case there is a misspelling mismatch, the whole city name list will be printed to standard output.

## Export
The program prints to standard output the number of result webpages for each search and the total number of ads scraped and parsed.  
It exports a JSON file with a list of dictionaries. Each dictionary contains the details for one ad.
A log file is also generated during the program run.

## To do
Next version will create a SQL database and populate it, according to the Entity Relationship Diagram in ERD.pdf.


## Features

The data scraped from each ad are:
- city
- address
- rooms
- size in $m^2$
- floor
- floors in bluiding
- number of pictures
- entry date
- condo fee
- city property tax (arnona)
- contact type: private individual or real estate agent
    - if agent: name, office, phone
- price: dated so user can check evolution
- booleans:
    - safe room
    - balcony
    - sotoreroom
    - security bars
    - air conditioning
    - furniture
    - accessibility
    - elevator
    - parking
    - roommate compatible
    - pets allowed
    - sun boiler

## What the program does "Behind the scenes"

1. check user parameters.
2. get all quicklink pages, with one link per city for a given (property_type, ad_type) pair. For example, there may be 96 cities with ads for the search: 'regular_apartment, for sale'.
3. get the first result page with links for detailed ad pages for a given city. This page has info on other result pages, so we can scrape all result pages. For example, there may be 7 result pages for the search: 'regular_apartment, for sale in Jerusalem'.
4. get detailed ad pages. There are up to 20 ad links per 2nd-level page.
5. parse pages, extract data.
6. export JSON with results.

## Contact Info
Yosef Mentzer - yosef.mentzer@gmail.com
