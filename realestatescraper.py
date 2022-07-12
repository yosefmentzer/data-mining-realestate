from bs4 import BeautifulSoup
import requests
import logging
import sys
import pandas as pd

# constants
YAD2 = 'https://www.yad2.co.il/realestate/forsale?topArea=19&area=42&city=8700'
MADLAN = 'https://www.madlan.co.il/'

CLASS = 'tblModaa round6 shadowPres CardStyle tblModaaArrow'
FLAVOR = 'bs4'

KOMO = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?nehes=1&cityName=%D7%A8%D7%A2%D7%A0%D7%A0%D7%94'
KOMO = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?nehes=1&cityName=%D7%AA%D7%9C+%D7%90%D7%91%D7%99%D7%91+%D7%99%D7%A4%D7%95'
KOMO = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?nehes=1&cityName=%D7%99%D7%A8%D7%95%D7%A9%D7%9C%D7%99%D7%9D'
KOMO = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?nehes=1&cityName=%D7%97%D7%99%D7%A4%D7%94'
KOMO = 'https://www.komo.co.il/code/nadlan/apartments-for-sale.asp?nehes=1&cityName=%D7%A8%D7%9E%D7%AA+%D7%92%D7%9F'

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


def main():
    # Part 1
    r = get_response(KOMO)
    if r:
        logger.info(f'Successful response.{r.url}')
    else:
        logger.error(f'Request error: {r}.{r.url}')
        return
    # soup = parse_response(r)
    # df_list = pd.read_html(soup.get_text())
    # tables = soup.find_all('table')
    dfs = pd.read_html(KOMO, attrs={'class': CLASS}, flavor=FLAVOR)


if __name__ == '__main__':
    main()
