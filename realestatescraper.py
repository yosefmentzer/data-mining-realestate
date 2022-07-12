"""
realestatescraper.py
scrape ads of apartments for sale from https://www.komo.co.il,
print results, and export csv file.
"""

from bs4 import BeautifulSoup
import requests
import logging
import sys
import pandas as pd
import urllib.parse

# constants
CLASS = 'tblModaa round6 shadowPres CardStyle tblModaaArrow'
FLAVOR = 'bs4'

KOMO = 'https://www.komo.co.il'
KOMO_LIST = 'https://www.komo.co.il/code/nadlan/quick-links.asp?nehes=1&subLuachNum=2'
SALE_URL_SUBSTRING = '/code/nadlan/apartments-for-sale.asp'
TAIL_SALE_URL_SUBSTRING = '?nehes=1&cityName='

PAGE_SUBSTRING = '?currPage'
HEAD_PAGE_URL = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?currPage='
TAIL_PAGE_URL = '&subLuachNum=2&nehes=1&cityName='

COLUMNS_TO_DROP = ['index', 4]

EXPORT_FILENAME = 'realestate_data.csv'

# logger setup
logger = logging.getLogger('scraper')
logger.setLevel(logging.DEBUG)

# Create Formatter
formatter = logging.Formatter(
    '%(asctime)s-%(levelname)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')

# create a file handler and add it to logger
file_handler = logging.FileHandler('realestate.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
# create a stdout handler and add it to logger
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.ERROR)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_response(url):
    """
    send a GET request to the specified url
    :param url: url
    :return: Response object
    """
    r = requests.get(url)

    return r


def parse_response(r):
    """
    parse (get soup object from) response
    :param r: response object
    :return: parsed soup object
    """
    html_doc = r.text
    soup = BeautifulSoup(html_doc, "html.parser")

    return soup


def get_citynames(soup):
    """
    take soup object from URL of page with list of cities and links (KOMO_LIST),
    return list of city names for which there are apartment sale ads.
    :param soup: parsed soup object from URL of page with list of cities and links (KOMO_LIST)
    :return: list of citynames
    """
    citynames = []
    for link in soup.find_all('a'):
        if SALE_URL_SUBSTRING in link.get('href'):
            if link.get('cityname'):
                citynames.append(link.get('cityname'))

    return citynames


def get_urls(citynames):
    """
    take list of citynames and build URLs according to website's patterns.
    get quoted city name, as city names are in Hebrew.
    :param citynames: list of citynames
    :return: list of tuples. In each tuple (cityname, cityname_quoted and url)
    """
    cities_urls = []
    for cityname in citynames:
        cityname_quoted = urllib.parse.quote(cityname)  # Hebrew letters must be quoted
        url = ''.join([KOMO, SALE_URL_SUBSTRING, TAIL_SALE_URL_SUBSTRING, cityname_quoted])
        cities_urls.append((cityname, cityname_quoted, url))

    return cities_urls


def get_pages(soup):
    """
    take parsed soup object of first page of city's website,
    get list of all other pages with ads.
    Example: pages list ['2', '3', '4'] indicate that there are three pages
    besides the first one for the given city.
    :param soup: parsed soup object of first page of city's website
    :return: pages list
    """
    ls = []
    for href in soup.find_all('a'):
        if 'currPage' in href.get('href'):
            ls.append(href.string)
    pages = [elt for elt in ls if elt.isnumeric()]

    return pages


def get_url_for_page(page, cityname_quoted):
    """
    take page number and cityname_quoted and build url for the page.
    :param page: page number
    :param cityname_quoted: quoted city name, as city names are Hebrew.
    e.g. ירושלים -> %D7%99%D7%A8%D7%95%D7%A9%D7%9C%D7%99%D7%9D
    :return: url for the page
    """
    page_url = ''.join([HEAD_PAGE_URL, page, TAIL_PAGE_URL, cityname_quoted])

    return page_url


def get_df_from_url(url, cityname):
    """
    take url and city name, read the webpage into a pandas df,
    clean df columns, rename columns, insert column with city name
    :param url: url of website with ads
    :param cityname: city name
    :return: pandas df with ads data
    """
    dfs = pd.read_html(url, attrs={'class': CLASS}, flavor=FLAVOR)
    df_list = [df[0] for df in dfs]
    df = pd.DataFrame(df_list)
    df = df.reset_index()
    df = df.drop(columns=COLUMNS_TO_DROP)
    df.columns = ['pictures', 'description1', 'price', 'description2']
    df['cityname'] = cityname
    df = df[['cityname', 'pictures', 'description1', 'price', 'description2']]

    return df


def print_df(df, cityname):
    """
    take df and its city name, print results.
    :param df: df with ads data
    :param cityname: city name
    """
    print(f'city name: {cityname}')
    for i in range(len(df)):
        print(f'Record number: {i+1} for city {cityname}')
        for col in df.columns[1:]:  # first column is 'city name', already printed.
            print(col, ': ', df.iloc[i][col])
        print('\n')


def main():

    r = get_response(KOMO_LIST)
    if r:
        logger.info(f'Successful response.{r.url}')
    else:
        logger.error(f'Request error: {r}.{r.url}')
        return
    soup = parse_response(r)
    if soup:
        logger.info(f'Successful parsing.{r.url}')
    else:
        logger.error(f'Parsing error. {r.url}')
    citynames = get_citynames(soup)
    if citynames:
        logger.info(f'Got {len(citynames)} citynames')
    else:
        logger.error('Error getting citynames.')
    cities_urls = get_urls(citynames)
    if cities_urls:
        logger.info(f'Got {len(cities_urls)} cities_urls')
    else:
        logger.error('Error getting cities_urls.')

    cities_df_list = []
    for cityname, cityname_quoted, url in cities_urls:
        print(f'Loading data for city: {cityname}...')
        df_list = []
        try:
            df = get_df_from_url(url, cityname)
            logger.info(f'Got df from {url} with {len(df)} rows.')
            df_list.append(df)
        except Exception as e:
            logger.error(f'Error getting df from {url}. {str(e)}')
            continue
        r = get_response(url)
        if r:
            logger.info(f'Successful response.{r.url}')
        else:
            logger.error(f'Request error: {r}.{r.url}')
            continue
        soup = parse_response(r)
        if soup:
            logger.info(f'Successful parsing.{r.url}')
        else:
            logger.error(f'Parsing error. {r.url}')
        pages = get_pages(soup)
        if pages:
            logger.info(f'Got pages for {url}')
        for page in pages:
            try:
                page_url = get_url_for_page(page, cityname_quoted)
                logger.info(f'Got url for page {page}.')
            except Exception as e:
                logger.error(f'Error getting url for page {page}. {str(e)}')
                continue
            try:
                df = get_df_from_url(page_url, cityname)
                logger.info(f'Got df for page {page}.')
                df_list.append(df)
            except Exception as e:
                logger.error(f'Error getting df for page {page}. {str(e)}')

        city_df = pd.concat(df_list)
        print_df(city_df, cityname)
        cities_df_list.append(city_df)
    cities_df = pd.concat(cities_df_list)

    logger.info(f'Finished scraping.')

    cities_df.to_csv(EXPORT_FILENAME)


if __name__ == '__main__':
    main()
