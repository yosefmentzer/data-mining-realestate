# realestatescraper.py
## Apartments' selling ads' scraper program - from the site www.komo.co.il 


realestatescraper.py is a program that helps the user to scrape all apartments' selling ads from the site komo.co.il. 
By running the program, the user gets a csv file, containing all ads of apartments for sale, that he can easily manipulate on his computer.  


## Features

The program exports data of all ads of apartments for sale at the site komo.co.il, to a csv file. 
The data scraped from each apartment's ad are:
- (unnamed column): index of the apartment in the page from which the data were scraped.
- cityname: The city of the apartment.
- pictures: how many pictures are connected to the ad.
- description1: address of the apartment, number of rooms.
- price: asked price of the apartment
- description2: size of the apartment (e.g. 85m2), number of rooms, floor (e.g. 3rd floor).
The description columns are strings scraped 'as is', without further processing. So, for example, the number of rooms appear in both descripion1 and description2.
Next version will include parsing of these columns.

A log file is also generated during the program run.

## Requirements

The libraries that are required for using realestatescraper.py are listed in the requirements.txt file.


## Installation

realestatescraper.py and requirements.txt can be downloaded from https://github.com/yosefmentzer/data-mining-realestate


## Usage instructions

In order to scrape the data of the apartments' selling ads:
- Download realestatescraper.py and requirements.txt from https://github.com/yosefmentzer/data-mining-realestate
- Make sure that all required libraries which are witten in the requirements.txt file are installed on your computer
- Run the program


## The process that the program runs "behind the scenes":

After running the program,
- The program accesses komo.co.il and collects all city names from the site's "quick-links" (page from the site).  
- For each city (from the city list), the program builds a URL, scrapes the data from the first page and identifies how many pages are left. 
- If the city has several pages of ads, the program generates a different url for each page and scrapes the data from each page.
- The program parses the data into a pandas DataFrame.
- After finishing scraping and parsing the data for a city, the data in the city's DataFrame is printed to standard output.
- After all cities' pages are scraped, the DataFrames for each city are concatenated into one aggregated DataFrame and exported as a csv file.
- All process is monitored with a logging file.


## Future features

- Track after changes in ads (new ads, price change...)
- Parse the description strings into relevant elements (street, size, # rooms...)



## Contact Info

Yosef Mentzer - yosef.mentzer@gmail.com
Avishai Yossipovitch - aviyossi@gmail.com
