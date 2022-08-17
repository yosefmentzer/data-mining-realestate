# Real Estate scraper
Real Estate scraper is a program that scrapes ads from [KOMO](https://www.komo.co.il) and builds an SQL database with detailed information on each ad.  
Additionally, it queries a governmental [API](https://info.data.gov.il/datagov/home/) to get demographic data for the cities in the database. Cool!

## Preparation
- Download realestatescraper.py, config.py, createdb.py, updatedb.py, queryapi.py and *requirements.txt* from https://github.com/yosefmentzer/data-mining-realestate.
- Make sure that all required libraries in the `requirements.txt` file are installed in your system.
- prepare a `credentials.ini` file with the following structure:

> [DB]
host = localhost  
user = root  
password = your_MySQL_password

If your host or user is diffferent, set up the `credentials.ini` file accordingly.

- Run createdb.py to create the database structure. The database will be called `realestate`.

From the terminal:
```bash
python createdb.py
```
updatedb.py has code to update the database, queryapi.py has code to query the API and config.py has configuration/internal variables. They will be called by realestatescraper.py, so you don't have to worry about them. Just have them on your system.

## Usage
From the terminal:
```bash
python realestatescraper.py -p 1 -a 2 -c "Tel Aviv Yaffo" --api
```
Optional parameters:
-p, --prop: property type. 1 for regular apartment, 2 for two-family building, etc. If not provided, the scraper will scrape all. See full list of codes and property types in the help menu:
```bash
python realestatescraper.py --help
```
-a, --ad: advertisement type. 1 for sale, 2 for rent. If not provided, the scraper will scrape both.  
-c, --city: city name in English. In case there is a misspelling mismatch, the whole city name list will be printed to standard output. If not provided, the scraper will scrape all cities.  
-t, --tsize: transaction size. If not provided, the program will commit 300 transactions at a time when updating the database.  
--api: flag to download demographic data from API or not.  
--onlyapi: flag to **only** download demographic data from API and thus **not scrape**. In this case all scraping-related params are ignored.

## What the program does
The program scrapes all [KOMO](https://www.komo.co.il) relevant ad pages, according to the parameters provided (or default values if nor provided).
It prints to standard output the number of result webpages for each search and the total number of ads scraped and parsed.  
The program updates the MySQL database with the scraped data.
If a record already exists in the database, only new data will be stored.  
Prices and the date are always stored, so that the user can track price evolution.
If the API option is flagged, the program queries the [Israeli governmental data API](https://data.gov.il/) and gets demographic data for the cities in the `realestate` database.
A log file is also generated during the program run.

## The database
Check the Entity Relationship Diagram (ERD.pdf) for a visual representation of the database tables, their relationships and columns.


## The tables and their columns

**properties**
- id
- website_id: property id in [KOMO](https://www.komo.co.il)
- property_type_id: 1 for regular apartment, 2 for two-family building, etc.
- ad_type_id: 1 for rent and 2 for sale
- city_id
- contact_id: id of the person that advertised the property

**property_details**
- property_id
- address
- neighborhood: string
- rooms: number of rooms (e.g. 2, 3.5)
- size_m2: size in $m^2$
- floor_property: floor of the property (e.g. "ground", 3, 6) 
- floors_in_building: total floors in the building
- description: text description by the announcer, limited to 1000 characters.
- entry_date: text or an actual date. (e.g. "flexible", '01/12/2022')  
- condo_fee:
- arnona: municipal property tax
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
    
**contacts**
- id:  we have access to details of real estate agents only, so every real estate agent will be a record in in the contacts table. There is one record for 'private individual', as the website does not give us contact details for them and it would not be efficient to create many identical semi-empty records in the contacts table.
- website_id: id in [KOMO](https://www.komo.co.il), if agent
- contact_type: private individual or agent (text)
- office: office name, if agent
- name: name, if agent
- phone: phone, if agent

**property_types**
- id
- website_id: property type id in [KOMO](https://www.komo.co.il)
- name: 'regular_apartment',  'two_family',  'penthouse',  'garden_apartment',  'house',  'duplex', 'studio',  or 'dwelling_unit'

**ad_types**
- id
- website_id: ad type id in [KOMO](https://www.komo.co.il)
- name: rent or sale

**cities**
- id
- name_heb: city name in Hebrew as extracted from website
- name_eng: [KOMO](https://www.komo.co.il) does not provide city names in English. 96 city names were translated ex ante (see config.CITIES) so that the user can use a --city CLI param to limit the search.  If the scraper finds new records for city names on top of the preloaded 96-city list, a new record with a Hebrew name alone will be created.

**prices**
- id: prices are always considered a new record--so user can track them--even if scraped twice in the same day (could have changed)
- property_id
- date
- price

**demographics**
- city_id: id of the city in `realestate` database. The program only stores data for relevant cities.
- total_pop: total population
- age_0_5, age_6_18, age_19_45, age_46_55, age_56_64, age_65_plus: population per group age

## What the program does "Behind the scenes"

1. check user parameters.
2. get all quicklink pages, with one link per city for a given (property_type, ad_type) pair. For example, there may be 96 cities with ads for the search: 'regular_apartment, for sale'.
3. get the first result page with links for detailed ad pages for a given city. This page has info on other result pages, so we can scrape all result pages. For example, there may be 7 result pages for the search: 'regular_apartment, for sale in Jerusalem'.
4. get detailed ad pages. There are up to 20 ad links per result page.
5. parse pages, extract data.
6. update database tables with results.
7. query API, get records.
8. Insert new records or update current records in `demographics` table.