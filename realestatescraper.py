"""
Real Estate scraper: program to scrape real estate ads from https://www.komo.co.il,
This module scrapes details for each ad for a search according to CLI parameters,
and populates a SQL database, storing only new data.
Module also queries data.gov.il API, gets demographics data and populate demographics table in database.
"""


from bs4 import BeautifulSoup
import grequests
import requests
import logging
import urllib.parse
import json
import re
import unicodedata
import itertools
from datetime import date
import argparse
import configparser

import config
import updatedb
import queryapi


# logger setup
logger = logging.getLogger('scraper')
logger.setLevel(logging.DEBUG)

# Create Formatter
formatter = logging.Formatter(
    '%(asctime)s-%(levelname)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')

# create a file handler and add it to logger
file_handler = logging.FileHandler('realestate.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def parse_args():
    """
    define command arguments and return a parsed args variable.
    :return: args
    """
    parser = argparse.ArgumentParser(description='Real Estate ads scraper.')
    parser.add_argument('-p', '--prop', type=int, help=f'property type: {config.PROPERTY_TYPES}')
    parser.add_argument('-a', '--ad', type=int, help=f'advertisement type: {config.AD_TYPES}')
    parser.add_argument('-c', '--city', type=str, help='city name in English. e.g. Jerusalem')
    parser.add_argument('-t', '--tsize', default=config.DEFAULT_TSIZE, type=int,
                        help=f'transaction size: number of records per transaction in SQL DB')
    parser.add_argument('--api', action='store_true', help='flag: download demographic data from API')
    parser.add_argument('--onlyapi', action='store_true',
                        help='flag: only download demographic data from API and do not scrape.\n'
                             'All scraping-related params will be ignored.')
    args = parser.parse_args()

    return args


def check_args():
    """
    Check CLI arguments returned by parse_args().
    If any argument is invalid, print message and return None.
    :return: property_types, ad_types, city_param, tsize, api, onlyapi
    property_type chosen by user in CLI or all config.PROPERTY_TYPES if no user param.
    ad_type chosen by user in CLI or all config.AD_TYPES if no user param.
    city_param chosen by user in CLI or None if no user param (in this case we'll scrape all cities).
    tsize chosen by user in CLI or default value if no user param.
    api is Boolean reflecting user decision to query demographics API or not. Default is False.
    onlyapi is Boolean reflecting user decision to **only** query demographics API or not.
    In this case all scraping-related params are ignored. Default is False.
    """
    args = parse_args()

    if args.ad:
        if args.ad in config.AD_TYPES:
            ad_types = {k: v for k, v in config.AD_TYPES.items() if k == args.ad}
        else:
            print('The ad type informed is not valid. Please try again.')
            return
    else:
        ad_types = config.AD_TYPES
    if args.prop:
        if args.prop in config.PROPERTY_TYPES:
            property_types = {k: v for k, v in config.PROPERTY_TYPES.items() if k == args.prop}
        else:
            print('The property type informed is not valid. Please try again.')
            return
    else:
        property_types = config.PROPERTY_TYPES
    if args.city:
        if args.city.title() in config.CITIES:
            city_param = args.city.title()
        else:
            print(f'The city informed is invalid.\n'
                  f'It may be spelled in our records differently from your input.\n'
                  f'The full list of eligible cities is:\n{sorted(list(config.CITIES.keys()))}')
            return
    else:
        city_param = None

    if args.tsize < 1:
        print('The transaction size informed is not valid. Please try again.')
        return
    tsize = args.tsize
    api = args.api
    onlyapi = args.onlyapi
    if onlyapi:
        print('You chose to only query the API and not to scrape ads.\n'
              'In case you entered valid scraping-related parameters, they will be ignored.\n')
        api = True

    return property_types, ad_types, city_param, tsize, api, onlyapi


def get_quicklinks(property_types, ad_types):
    """
    get quicklinks for all valid combinations of property_types and ad_types.
    the 'quicklinks webpage' is a page with one link per city for a given (property_type, ad_type) pair.
    It is equivalent to choosing "show all cities" in the main site's search bar.
    Example: https://www.komo.co.il/code/nadlan/quick-links.asp?nehes=1&subLuachNum=1
    :param property_types: dictionary {id: 'property_type'} for the property types to be searched.
                            if not specified by user, all property types will be searched.
                            See config.PROPERTY_TYPES
    :param ad_types: idem, for advertisement types (sale or rent).
                            See config.AD_TYPES
    :return: dictionary: {(property_type, ad_type): link}
                        Example: {(1, 1): 'https://www.komo.co.il/code/nadlan/quick-links.asp?nehes=1&subLuachNum=1'}
    """
    quicklinks = {elt: f'https://www.komo.co.il/code/nadlan/quick-links.asp?nehes={elt[0]}&subLuachNum={elt[1]}'
                  for elt in itertools.product(property_types.keys(), ad_types.keys())}

    return quicklinks


def get_response(url):
    """
    send a GET request to the specified url using requests.
    :param url: url
    :return: Response object
    """
    r = requests.get(url)

    return r


def get_responses_grequests(urls):
    """
    send a GET request to the specified urls using grequests (asynchronously).
    :param urls: url list
    :return: list of Response objects
    """
    reqs = [grequests.get(url) for url in urls]
    responses = grequests.map(reqs)

    return responses


def parse_response(r):
    """
    parse (get soup object from) response object.
    :param r: response object
    :return: parsed soup object
    """
    html_doc = r.text
    soup = BeautifulSoup(html_doc, "html.parser")

    return soup


def get_city_urls(city_param, property_type, soup):
    """
    get urls for the cities that show up in a quicklink webpage.
    limit result to city_param in case user specified a city to limit the search.
    :param city_param: city (in English) specified by the user to limit the search or None if not specified.
                        (in this case we will scrape all cities that show up in the quicklink page).
                        See config.CITIES.
    :param property_type: property_type of current search. See config.PROPERTY_TYPES
    :param soup: parsed response of current quicklink webpage.
    :return: {cityname: url}. cityname in Hebrew, as it is extracted from quicklink webpage
                                and optionally limited by city_param.
    """
    if city_param:
        city_urls = {elt.a.get('cityname'): ''.join(['https://www.komo.co.il/',  # domain
                                                     elt.a.get('href').split('?')[0],  # route before params
                                                     '?nehes=',  # first param name
                                                     str(property_type),  # first param value
                                                     '&cityname=',  # second param name
                                                     urllib.parse.quote(elt.a.get('cityname'))  # second param value.
                                                     ])
                     for elt in soup.find_all('div', attrs={'class': 'listFloatItem'})
                     if elt.a.get('cityname') == config.CITIES[city_param]}
    else:
        city_urls = {elt.a.get('cityname'): ''.join(['https://www.komo.co.il/',
                                                     elt.a.get('href').split('?')[0],
                                                     '?nehes=',
                                                     str(property_type),
                                                     '&cityname=',
                                                     urllib.parse.quote(elt.a.get('cityname'))
                                                     ])
                     for elt in soup.find_all('div', attrs={'class': 'listFloatItem'})}

    return city_urls


def get_pages(soup):
    """
    get pages of a search result, given the (parsed) first result page.
    Example: ['2', '3', '4'] in case there are 4 result pages.
    :param soup: (parsed) first result page.
    :return: list of pages of a search result.
    """
    pages = [elt.string for elt in soup.find_all('a', attrs={'class': 'paging'}) if elt.string.isnumeric()]

    return pages


def get_page_urls(city_url, pages, ad_type):
    """
    get urls for all pages in a search result for a given city and a given advertisement type.
    Take the url to the first page (city_url), the list of pages of the search result (excluding the first one),
    and the advertisement type 1 or 2. {1: 'rent', 2: 'sale'}
    The ad_type is necessary because the url to the first result page (city_url) does not contain the ad_type
    and the urls from page 2 on include the ad_type.
    :param city_url: the url to the first page of the search results.
    :param pages: list of pages of a search result. Example: ['2', '3', '4'] in case there are 4 pages.
    :param ad_type: advertisement type 1 or 2. {1: 'rent', 2: 'sale'}
    :return: list of urls
    """
    page_urls = [''.join([city_url.split('?')[0], '?',
                          f'currPage={page}&subLuachNum={ad_type}&',
                          city_url.split('?')[1]])
                 for page in pages]

    return page_urls


def get_ad_ids(soup):
    """
    get the ad ids in a parsed page of search result.
    Example: get the ids for all the ads in
    https://www.komo.co.il/code/nadlan/apartments-for-rent.asp?nehes=1&cityName=%D7%91%D7%90%D7%A8+%D7%A9%D7%91%D7%A2
    :return: list of ad ids in the page
    """
    ad_ids = [elt.get('id').split('modaaRowDv')[-1]
              for elt in soup.find_all('div', attrs={'id': re.compile('modaaRowDv')})]

    return ad_ids


def get_ad_url(ad_id):
    """
    given an ad_id, get the url to detailed ad page.
    Equivalent to clicking on the ad pop-up link.
    Example: https://www.komo.co.il/code/nadlan/details/?modaaNum=3987676 for ad_id '3987676'.
    :param ad_id: ad id (str)
    :return: url to detailed ad page
    """
    ad_url = ''.join(['https://www.komo.co.il/code/nadlan/details/?modaaNum=', ad_id])

    return ad_url


def parse_detailed_ad_page(soup, ad_id, property_type, ad_type, today):
    """
    parse detailed ad page and get details.
    :param soup: parsed ad page
    :param ad_id: ad id
    :param property_type: property_type of current search. See config.PROPERTY_TYPES
    :param ad_type: ad_type of current search. See config.AD_TYPES
    :param today: today date in iso format
    :return: dictionary with ad details.
    """
    details = {'ad_id': ad_id, 'property_type': property_type, 'ad_type': ad_type, 'date': today}

    # get tag and string with address. If there is no address, register 'לא צוינה כתובת'
    addresstop_tag = soup.find(attrs={'class': 'addressTop'})
    # the string comes with \xa0 instead of some spaces, so we need to normalize it.
    # Example: before normalization: 'דירה להשכרה,\xa0פרופ’ מחרז אברהם\xa04'
    #          after normalization: 'דירה להשכרה, פרופ’ מחרז אברהם 4'
    if addresstop_tag:
        addresstop_fulltext = unicodedata.normalize("NFKD", addresstop_tag.find('span').string.strip())
        details['address'] = (addresstop_fulltext.split(', ')[-1]
                              if len(addresstop_fulltext.split(', ')) > 1
                              else 'לא צוינה כתובת')
    else:
        details['address'] = 'לא נשלף'

    # get tag and string with neighborhood. If there is no neighborhood, register 'לא צוינה שכונה'
    addresbottom_tag = soup.find('div', attrs={'class': 'addresBottom'})
    if addresbottom_tag:
        addresbottom_fulltext = unicodedata.normalize("NFKD", addresbottom_tag.find('span').string.strip())
        details['neighborhood'] = (addresbottom_fulltext.split(', ')[0]
                                   if len(addresbottom_fulltext.split(', ')) > 1
                                   else 'לא צוינה שכונה')
    else:
        addresbottom_fulltext = None
        details['neighborhood'] = 'לא נשלף'

    # record city name as extracted from ad page.
    if addresbottom_fulltext:
        details['city'] = addresbottom_fulltext.split(', ')[-1]
    else:
        details['city'] = 'לא נשלף'

    # get price
    if soup.find(attrs={'class': re.compile("ModaaWDetailsValue")}):
        price_raw = soup.find(attrs={'class': re.compile("ModaaWDetailsValue")}).string
    else:
        price_raw = None
    # exclude the shekel symbol and commas from the string.
    # ads with no price info have "לא צוין מחיר" in this field and map to None.
    if price_raw:
        if re.sub("[^0-9]", "", price_raw):
            price = re.sub("[^0-9]", "", price_raw)
        else:
            price = None
    else:
        price = None
    details['price'] = price

    # get tag and strings with data on rooms, floor of the property, size in m2 and entry date.
    firstinfo_tags = soup.find_all('div', attrs={'class': 'firstInfo'})
    if firstinfo_tags:
        # rooms may be fractional, such as '2.5'
        details['rooms'] = (firstinfo_tags[0].string.strip())
        # floor is normally numeric, but may be "קרקע"
        details['floor_property'] = (firstinfo_tags[1].string.strip())
        details['size_m2'] = (firstinfo_tags[2].string.strip())
        # entry dates may be a string like "גמיש" or an actual date.
        details['entry_date'] = firstinfo_tags[3].string.strip() if len(firstinfo_tags) > 3 else None
    else:
        details['rooms'] = None
        details['floor_property'] = None
        details['size_m2'] = None
        details['entry_date'] = None

    # get text description, total floors in building, condo fee and city property tax (arnona)
    details['description'] = (soup.find('div', attrs={'id': 'teurWrap'}).string.strip()
                              if soup.find('div', attrs={'id': 'teurWrap'})
                              else None)
    details['floors_total'] = (soup.find('li', attrs={'class': 'floorTotal'}).strong.string
                               if soup.find('li', attrs={'class': 'floorTotal'})
                               else None)
    details['condo_fee'] = (re.sub("[^0-9]", "", soup.find('li', attrs={'class': 'vaadBait'}).strong.string)
                            if soup.find('li', attrs={'class': 'vaadBait'})
                            else None)
    details['arnona'] = (re.sub("[^0-9]", "", soup.find('li', attrs={'class': 'arnona'}).strong.string)
                         if soup.find('li', attrs={'class': 'arnona'})
                         else None)

    # get values for boolean features, such as saferoom, balcony, etc.
    for feature in config.BOOLEAN_FEATURES:
        if soup.find(attrs={'class': f'{feature} add'}):
            details[feature] = True
        else:
            details[feature] = False

    # get tag with contact type: private individual or real estate agent
    contact_tag = soup.find('div', attrs={'class': re.compile("mefarsemNew")})
    details['contact_type'] = contact_tag.string.strip()
    # for real estate agents, get office name and website id
    if soup.find('span', attrs={'class': 'misradName'}):
        details['contact_office'] = soup.find('span', attrs={'class': 'misradName'}).string
        details['contact_website_id'] = soup.find('span', attrs={'class': 'misradName'}).get('minisitenum')
    # luach number and modaa number are parameters for an API request to get real estate agent's phone.
    # modaa number should match ad id, but we extract here as is.
    luachnum = eval(contact_tag['onclick'].split('ModaotActions.modaaWShowPhoneBottom')[1].split(';')[0])[0]
    modaanum = eval(contact_tag['onclick'].split('ModaotActions.modaaWShowPhoneBottom')[1].split(';')[0])[1]

    # to get real agent's email or private announcer phone or email, one must be logged in website's system.
    # so that would be beyond the scope of this scraper.
    # Thus, we get agent's name and phone alone (in case announcer is an agent)
    if details['contact_type'] == 'מפרטי':
        details['contact_name'] = None
        details['contact_phone'] = None
    elif details['contact_type'] == 'מתיווך':
        # API request for agent's phone, equivalent to clicking on the 'phone' button on the ad site.
        # agent phone and name are not displayed on the website.
        # We must do a separate API request simulating a click.
        agent_details = get_agent_details(luachnum, modaanum)  # API request for agent's phone
        if agent_details['status'] == 'OK':
            details['contact_name'] = agent_details['data']['name']
            details['contact_phone'] = ''.join([agent_details['data']['phone1_pre'], agent_details['data']['phone1']])
        else:
            details['contact_name'] = None
            details['contact_phone'] = None

    return details


def get_agent_details(luachnum, modaanum):
    """
    get API response (JSON) for the contact details of an agent.
    Take params luach number and modaa number (defined by website's API).
    :param luachnum: param luach number
    :param modaanum: param modaa number
    :return: dictionary with agent_details
    """
    url = ''.join(['https://www.komo.co.il/api/modaotActions/showPhone.api.asp', '?',
                   'luachNum=', str(luachnum), '&', 'modaaNum=', str(modaanum)])
    r = get_response(url)
    agent_details = json.loads(r.text)

    return agent_details


def scrape(property_types, ad_types, city_param):
    """
    this function performs the scraping activity.
    :param property_types: property types to scrape.
    :param ad_types: advertisement types to scrape.
    :param city_param: city to scrape (if not provided, scrape all cities).
    :return: dictionary of dictionaries {'ad_id': details_dictionary},
    """
    print('This is the Real Estate scraper.\n'
          'Running time may vary from a few minutes to dozens of minutes depending on the parameters provided.\n'
          'Scraping all the site for all possible property types, ad types and cities may take a few hours.\n')

    # the date will be registered.
    today = date.today().isoformat()

    # the scraper will return a dictionary of dictionaries.
    # Each nested dictionary has details of an ad. {'ad_id': details_dictionary}
    details_dic = {}

    # There are three levels of page results until we get the detailed page for a specific ad.
    # 1st level: the 'quicklinks webpage', with one link per city for a given (property_type, ad_type) pair.
    #            For example, there may be 96 cities with ads for the search: 'regular_apartment, for sale'.
    #            Example: https://www.komo.co.il/code/nadlan/quick-links.asp?nehes=1&subLuachNum=1
    # 2nd level: the first result page with links for detailed ad pages for a given city.
    #            this page has info on other result pages, so we can scrape all result pages.
    #            For example, there may be 7 result pages for the search: 'regular_apartment, for sale in Jerusalem'.
    #            Example: https://www.komo.co.il/code/nadlan/apartments-for-rent.asp?nehes=1&cityName=%D7%9C%D7%95%D7%93
    # 3rd level: detailed ad pages. There are up to 20 ad links per 2nd-level page.
    #            Example: https://www.komo.co.il/code/nadlan/details/?modaaNum=3865660

    quicklinks = get_quicklinks(property_types, ad_types)  # dictionary: {(property_type, ad_type): link}
    for (property_type, ad_type), link in quicklinks.items():
        logger.info(f'URL: {link}')
        # 1st level
        r = get_response(link)
        if not r:
            continue
        soup = parse_response(r)

        city_urls = get_city_urls(city_param, property_type, soup)  # dictionary {cityname: url}
        if not city_urls:
            print('This search did not match any result page in the website.\n'
                  'Try again with different parameters.\n')
            return
        city_urls_inv = {v: k for k, v in city_urls.items()}

        print(f'Scraping {len(city_urls)} city page(s) for '
              f'{config.PROPERTY_TYPES[property_type], config.AD_TYPES[ad_type]}.\n'
              f'Please wait...')

        resps_cities = get_responses_grequests(city_urls.values())
        if not resps_cities:
            continue
        resps_cities_dic = {resp: resp.url for resp in resps_cities}
        soup_cities_dic = {parse_response(resp): resp for resp in resps_cities_dic.keys()}

        for soup_city in soup_cities_dic.keys():
            city_url = resps_cities_dic[soup_cities_dic[soup_city]]
            cityname = city_urls_inv[city_url]
            # cityname reversed because name in Hebrew.
            # In regular terminal, displays correctly. In PyCharm terminal, displays inverted. Did not find out why.
            print(f'Loading data for city: {cityname[::-1]}')
            logger.info(f'URL: {city_url}')
            # 2nd level
            # get remainder result pages
            pages = get_pages(soup_city)
            page_urls = get_page_urls(city_url, pages, ad_type)  # list of urls for all pages of this city
            n_pages = max([int(elt) for elt in pages]) if pages else 1
            print(f'Number of result pages for the current search: {n_pages}.')

            # get ad_ids for the first result page (already parsed)
            ad_ids = get_ad_ids(soup_city)
            # get ad_urls
            ad_urls = {get_ad_url(ad_id): ad_id for ad_id in ad_ids}
            # 3rd level, detailed ad pages with GREQUESTS (for the ads in the first 2nd level result page)
            resps_ads_page1 = get_responses_grequests(ad_urls)
            if not resps_ads_page1:
                continue
            resps_ads_page1_dic = {resp: resp.url for resp in resps_ads_page1}
            soup_ads_page1_dic = {parse_response(resp): resp for resp in resps_ads_page1_dic.keys()}

            # parse all detailed ad pages (for the ads in the first 2nd level result page)
            for soup_ad_page1 in soup_ads_page1_dic.keys():
                ad_id = ad_urls[resps_ads_page1_dic[soup_ads_page1_dic[soup_ad_page1]]]
                details = parse_detailed_ad_page(soup_ad_page1, ad_id, property_type, ad_type, today)
                details_dic[ad_id] = details

            # scrape and parse all detailed ad pages for remainder 2nd level result pages (2nd on)
            resps_pages = get_responses_grequests(page_urls)
            if not resps_pages:
                continue
            resps_pages_dic = {resp: resp.url for resp in resps_pages}
            soup_pages_dic = {parse_response(resp): resp for resp in resps_pages_dic.keys()}
            for soup_pages in soup_pages_dic.keys():  # for page_url in page_urls:

                logger.info(f'URL: {resps_pages_dic[soup_pages_dic[soup_pages]]}')
                ad_ids = get_ad_ids(soup_pages)
                # get ad_urls
                ad_urls = {get_ad_url(ad_id): ad_id for ad_id in ad_ids}
                # 3rd level, detailed ad pages with GREQUESTS (for ads in remainder 2nd level result pages)
                resps_ads_otherpages = get_responses_grequests(ad_urls)
                if not resps_ads_otherpages:
                    continue
                resps_ads_otherpages_dic = {resp: resp.url for resp in resps_ads_otherpages}
                s_ads_otherpages_dic = {parse_response(resp): resp for resp in resps_ads_otherpages_dic.keys()}
                # parse all detailed ad pages (for ads in remainder 2nd level result pages)
                for s in s_ads_otherpages_dic.keys():
                    ad_id = ad_urls[resps_ads_otherpages_dic[s_ads_otherpages_dic[s]]]
                    details = parse_detailed_ad_page(s, ad_id, property_type, ad_type, today)
                    details_dic[ad_id] = details

    return details_dic


def feed_db_after_scraping(details_dic, tsize):
    """
    Take a dictionary with scraping results, and feed the database,
    inserting new records or updating current records.
    commit transactions according to transaction size.
    :param details_dic: dictionary with scraping results
    :param tsize: transaction size (defined by user or default value)
    """
    print(f'A total of {len(details_dic)} ads were scraped.\n')
    cred = configparser.ConfigParser()
    cred.read('credentials.ini')
    connection = updatedb.connect(cred)
    updatedb.use_db(connection)

    t = 0
    for ad_id, result in details_dic.items():
        if updatedb.query_db(f'SELECT id FROM properties WHERE website_id = {int(ad_id)}', connection):
            t = updatedb.update_current_add(int(ad_id), result, connection, t)
            if t > tsize:
                connection.commit()
                logger.info(f'Commited {t} transactions.')
                t = 0
        else:
            t = updatedb.insert_new_ad(ad_id, result, connection, t)
            if t > tsize:
                connection.commit()
                logger.info(f'Commited {t} transactions.')
                t = 0
    if t:
        connection.commit()
        logger.info(f'Commited {t} transactions.')

    connection.close()


def query_api_feed_db(tsize):
    """
    query API, get relevant results and feed the database,
    inserting new records or updating current records.
    commit transactions according to transaction size.
    :param tsize: transaction size (defined by user or default value)
    """
    t = 0
    cred = configparser.ConfigParser()
    cred.read('credentials.ini')
    connection = updatedb.connect(cred)
    updatedb.use_db(connection)
    api_records = queryapi.get_all_records(config.API_URL, config.API_DOMAIN)
    for record in api_records:
        t = updatedb.update_or_insert_demographics(record, connection, t)
        if t > tsize:
            connection.commit()
            logger.info(f'Commited {t} transactions.')
            t = 0
    if t:
        connection.commit()
        logger.info(f'Commited {t} transactions.')
    connection.close()


def main():
    check_args_result = check_args()
    if check_args_result:
        property_types, ad_types, city_param, tsize, api, onlyapi = check_args_result
    else:
        return
    if not onlyapi:
        details_dic = scrape(property_types, ad_types, city_param)
        if details_dic:
            feed_db_after_scraping(details_dic, tsize)
    if api:
        query_api_feed_db(tsize)


if __name__ == '__main__':
    main()
