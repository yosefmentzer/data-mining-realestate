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

    :param soup:
    :return:
    """
    citynames = []
    for link in soup.find_all('a'):
        if SALE_URL_SUBSTRING in link.get('href'):
            if link.get('cityname'):
                citynames.append(link.get('cityname'))

    return citynames


def get_urls(citynames):
    """

    :param citynames:
    :return:
    """
    cities_urls = []
    for cityname in citynames:
        cityname_quoted = urllib.parse.quote(cityname)  # Hebrew letters must be quoted
        url = ''.join([KOMO, SALE_URL_SUBSTRING, TAIL_SALE_URL_SUBSTRING, cityname_quoted])
        cities_urls.append((cityname, cityname_quoted, url))

    return cities_urls


def get_pages(soup):
    """

    :param soup:
    :return:
    """
    ls = []
    for href in soup.find_all('a'):
        if 'currPage' in href.get('href'):
            ls.append(href.string)
    pages = [elt for elt in ls if elt.isnumeric()]

    return pages


def get_url_for_page(page, cityname):
    """

    :param page:
    :param cityname:
    :return:
    """
    page_url = ''.join([HEAD_PAGE_URL, page, TAIL_PAGE_URL, cityname])

    return page_url


def get_df_from_url(url, cityname):
    """

    :param url:
    :return:
    """
    dfs = pd.read_html(url, attrs={'class': CLASS}, flavor=FLAVOR)
    df_list = [df[0] for df in dfs]
    df = pd.DataFrame(df_list)
    df = df.reset_index()
    df = df.drop(columns=['index', 4])
    df.columns = ['pictures', 'description1', 'price', 'description2']
    df['cityname'] = cityname
    df = df[['cityname', 'pictures', 'description1', 'price', 'description2']]

    return df


def print_df(df, cityname):
    """

    :param df:
    :return:
    """
    print(f'city name: {cityname}')
    for i in range(len(df)):
        print(f'Record number: {i+1} for city {cityname}')
        for col in df.columns[1:]:
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
        df = pd.DataFrame()
        try:
            df = get_df_from_url(url, cityname)
            logger.info(f'Got df from {url} with {len(df)} rows.')
        except Exception as e:
            logger.error(f'Error getting df from {url}. {str(e)}')
            continue

        if not df.empty:
            df_list.append(df)

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
            df = pd.DataFrame()
            try:
                df = get_df_from_url(page_url, cityname)
                logger.info(f'Got df for page {page}.')
            except Exception as e:
                logger.error(f'Error getting df for page {page}. {str(e)}')

            if not df.empty:
                df_list.append(df)

        # now we have city_df
        city_df = pd.concat(df_list)
        print_df(city_df, cityname)
        cities_df_list.append(city_df)
    cities_df = pd.concat(cities_df_list)

    logger.info(f'Finished scraping.')

    cities_df.to_csv('realestate_data.csv')


if __name__ == '__main__':
    main()
