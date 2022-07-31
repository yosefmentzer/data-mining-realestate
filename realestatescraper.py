"""
Real Estate scraper: program to scrape real estate ads from https://www.komo.co.il,
This module exports a JSON file with details for each ad for a search according to CLI parameters.
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

import config


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
    parser.add_argument('-t', '--tsize', type=int,
                        help=f'transaction size: number of records per transaction in SQL DB')
    args = parser.parse_args()

    return args


def check_args():
    """
    Check CLI arguments returned by parse_args().
    If any argument is invalid, print message and return None.
    :return: property_types, ad_types, city_param, tsize
    property_type chosen by user in CLI or all config.PROPERTY_TYPES if no user param.
    ad_type chosen by user in CLI or all config.AD_TYPES if no user param.
    city_param chosen by user in CLI or None if no user param.
    tsize chosen by user in CLI or None if no user param.
    """
    args = parse_args()
    # limit property_types and ad_types according to CLI params

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
        if args.city in config.CITIES:
            city_param = args.city
        else:
            print(f'The city informed is invalid.\n'
                  f'It may be spelled in our records differently than your input.\n'
                  f'The full list of cities is:\n{sorted(list(config.CITIES.keys()))}')
            return
    else:
        city_param = None
    if args.tsize:
        if args.tsize < 1:
            print('The transaction size informed is not valid. Please try again.')
            return
        tsize = args.tsize  # transaction size for use in SQL script
    else:
        tsize = None

    return property_types, ad_types, city_param, tsize


def get_quicklinks(property_types, ad_types):
    """
    get quicklinks for all valid combinations of property_types and ad_types.
    the 'quicklinks webpage' is a page with one link per city for a given (property_type, ad_type) pair.
    :param property_types:
    :param ad_types:
    :return: dictionary: {(property_type, ad_type): link}
    """
    quicklinks = {elt: f'https://www.komo.co.il/code/nadlan/quick-links.asp?nehes={elt[0]}&subLuachNum={elt[1]}'
                  for elt in itertools.product(property_types.keys(), ad_types.keys())}

    return quicklinks


def get_response(url):
    """
    send a GET request to the specified url
    :param url: url
    :return: Response object
    """
    r = requests.get(url)

    return r


def get_responses_grequests(urls):
    """
    send a GET request to the specified urls using grequests (asynchronously)
    :param urls: url list
    :return: list of Response objects
    """
    reqs = [grequests.get(url) for url in urls]
    responses = grequests.map(reqs)

    return responses


def parse_response(r):
    """
    parse (get soup object from) response
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
    :param city_param: city specified by the user to limit the search (optional)
    :param property_type: property_type of current search. See config.PROPERTY_TYPES
    :param soup: parsed response of current quicklink webpage.
    :return: {cityname: url}. cityname in Hebrew, as it is extracted from quicklink webpage.
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
    :param soup: (parsed) first result page.
    :return: list of pages of a search result. Example: ['2', '3', '4'] in case there are 4 pages.
    """
    pages = [elt.string for elt in soup.find_all('a', attrs={'class': 'paging'}) if elt.string.isnumeric()]

    return pages


def get_page_urls(city_url, pages, ad_type):
    """
    get urls for all pages in a search result for a given city and a given advertisement type.
    Take the url to the first page (city_url), the list of pages of the search result (excluding the first one),
    and the advertisement type 1 or 2. {1: 'rent', 2: 'sale'}
    :param city_url: the url to the first page of the search results
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
    :return: list of ad ids in the page
    """
    ad_ids = [elt.get('id').split('modaaRowDv')[-1]
              for elt in soup.find_all('div', attrs={'id': re.compile('modaaRowDv')})]

    return ad_ids


def get_ad_url(ad_id):
    """
    given an ad_id, get the url to detailed ad page.
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
    addresstop_tag = soup.find('div', attrs={'class': 'addressTop'})
    addresstop_fulltext = unicodedata.normalize("NFKD", addresstop_tag.find('span').string.strip())
    details['address'] = (addresstop_fulltext.split(', ')[-1]
                          if len(addresstop_fulltext.split(', ')) > 1
                          else 'לא צוינה כתובת')

    # get tag and string with neighborhood. If there is no neighborhood, register 'לא צוינה שכונה'
    addresbottom_tag = soup.find('div', attrs={'class': 'addresBottom'})
    addresbottom_fulltext = unicodedata.normalize("NFKD", addresbottom_tag.find('span').string.strip())
    details['neighborhood'] = (addresbottom_fulltext.split(', ')[0]
                               if len(addresbottom_fulltext.split(', ')) > 1
                               else 'לא צוינה שכונה')

    # record city name as extracted from ad page.
    details['city'] = addresbottom_fulltext.split(', ')[-1]

    # get tag and strings with data on rooms, floor of the property, size in m2 and entry date.
    firstinfo_tags = soup.find_all('div', attrs={'class': 'firstInfo'})
    details['rooms'] = (firstinfo_tags[0].string.strip())
    details['floor_property'] = (firstinfo_tags[1].string.strip())
    details['size_m2'] = (firstinfo_tags[2].string.strip())
    details['entry_date'] = firstinfo_tags[3].string.strip() if len(firstinfo_tags) > 3 else None

    # get text description, total floors in building, condo fee and city property tax (arnona)
    details['description'] = soup.find('div', attrs={'id': 'teurWrap'}).string.strip()
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
    # so we get agent's name and phone alone (in case announcer is an agent)
    if details['contact_type'] == 'מפרטי':
        details['contact_name'] = None
        details['contact_phone'] = None
    elif details['contact_type'] == 'מתיווך':
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
    Take params luach number and modaa number.
    :param luachnum: param luach number
    :param modaanum: param modaa number
    :return: dictionary with agent_details
    """
    url = ''.join(['https://www.komo.co.il/api/modaotActions/showPhone.api.asp', '?',
                   'luachNum=', str(luachnum), '&', 'modaaNum=', str(modaanum)])
    r = get_response(url)
    agent_details = json.loads(r.text)

    return agent_details


def scrape():
    """
    this is a wrapper function to perform the scraping activity as a whole,
    from getting CLI params to returning a list of ad details.
    :return: list of ad details
    """
    # check CLI arguments and return corresponding variables.
    if check_args():
        property_types, ad_types, city_param, tsize = check_args()
    else:
        # if check_args() returns None, there was an issue with CLI args and the scraper will close.
        return
    print('This is the Real Estate scraper. \n'
          'Running time may vary from a few minutes to a few hours depending on the parameters provided.\n')

    # the date will be registered, so we can follow prices.
    today = date.today().isoformat()

    # the scraper will return a list of dictionaries. Each dictionary has details of an ad.
    details_list = []

    # There are three levels of page results until we get the detailed page for a specific ad.
    # 1st level: the 'quicklinks webpage', with one link per city for a given (property_type, ad_type) pair.
    #            For example, there may be 96 cities with ads for the search: 'regular_apartment, for sale'
    # 2nd level: the first result page with links for detailed ad pages for a given city.
    #            this page has info on other result pages, so we can scrape all result pages.
    #            For example, there may be 7 result pages for the search: 'regular_apartment, for sale in Jerusalem'.
    # 3rd level: detailed ad pages. There are up to 20 ad links per 2nd-level page.

    quicklinks = get_quicklinks(property_types, ad_types)  # dictionary: {(property_type, ad_type): link}
    for (property_type, ad_type), link in quicklinks.items():
        logger.info(f'URL: {link}')
        # 1st level
        r = get_response(link)
        soup = parse_response(r)
        city_urls = get_city_urls(city_param, property_type, soup)  # dictionary {cityname: url}
        if not city_urls:
            print('This search did not match any result page in the website. Try again with different parameters.')
            return
        for cityname, url in city_urls.items():
            print(f'Loading data for '
                  f'{config.PROPERTY_TYPES[property_type]}, '
                  f'{config.AD_TYPES[ad_type]}, '
                  f'city: {cityname[::-1]}')  # reversed because name in Hebrew
            logger.info(f'URL: {url}')
            # 2nd level
            r = get_response(url)
            soup = parse_response(r)
            # get remainder result pages
            pages = get_pages(soup)
            page_urls = get_page_urls(url, pages, ad_type)  # list of urls for all pages of this city
            n_pages = max(pages) if pages else 1
            print(f'Number of result pages for the current search: {n_pages}.')

            # get ad_ids for the first result page (already parsed)
            ad_ids = get_ad_ids(soup)
            # get ad_urls
            ad_urls = {get_ad_url(ad_id): ad_id for ad_id in ad_ids}
            # 3rd level, detailed ad pages with GREQUESTS (for the first 2nd level result page)
            responses = get_responses_grequests(ad_urls)
            responses_dic = {resp: resp.url for resp in responses}
            soup_dic = {parse_response(resp): resp for resp in responses_dic.keys()}

            # parse all detailed ad pages for first 2nd level result page
            for soup in soup_dic.keys():
                ad_id = ad_urls[responses_dic[soup_dic[soup]]]
                details = parse_detailed_ad_page(soup, ad_id, property_type, ad_type, today)
                details_list.append(details)

            # scrape and parse all detailed ad pages for remainder 2nd level result pages (2nd on)
            for page_url in page_urls:
                logger.info(f'URL: {page_url}')
                r = get_response(page_url)
                soup = parse_response(r)
                ad_ids = get_ad_ids(soup)
                # get ad_urls
                ad_urls = {get_ad_url(ad_id): ad_id for ad_id in ad_ids}
                # 3rd level, detailed ad pages with GREQUESTS (for remainder 2nd level result pages)
                responses = get_responses_grequests(ad_urls)
                responses_dic = {resp: resp.url for resp in responses}
                soup_dic = {parse_response(resp): resp for resp in responses_dic.keys()}
                for soup in soup_dic.keys():
                    ad_id = ad_urls[responses_dic[soup_dic[soup]]]
                    details = parse_detailed_ad_page(soup, ad_id, property_type, ad_type, today)
                    details_list.append(details)

    return details_list


def main():
    details_list = scrape()
    if details_list:
        print(f'A total of {len(details_list)} ads were scraped.')
        with open("result.json", "w") as result:
            json.dump(details_list, result)


if __name__ == '__main__':
    main()
