"""
module to update Mysql database for Real Estate scraper project, given a list of results.
This module defines functions to be used by realestatescraper.py, thus there is no main() function.
"""

import pymysql.cursors


def connect(cred):
    """ establish connection, reading from credentials """
    connection = pymysql.connect(host=cred['DB']['host'],
                                 user=cred['DB']['user'],
                                 password=cred['DB']['password'],
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def use_db(connection):
    """ use 'realestate' database """
    with connection.cursor() as cursor:
        sql = 'USE realestate;'
        cursor.execute(sql)
        connection.commit()


def query_db(sql, connection):
    """ take string with sql query and a connection instance,
    query the database and return result (list) """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchall()
        return res


def insert_contact(data, connection):
    """ insert record to contacts table """
    with connection.cursor() as cursor:
        sql = 'INSERT INTO contacts ' \
              '(website_id, contact_type, office, name, phone) ' \
              'VALUES (%s, %s, %s, %s, %s);'
        cursor.execute(sql, data)


def insert_city(data, connection):
    """ insert record to cities table. Only Hebrew name is inserted, as there is no English name in the website.
    96 city names were translated ex ante so that the user can use a --city CLI param to limit the search.
    If the scraper finds new records for city names, they are normally small settlements/kibbutzim."""
    with connection.cursor() as cursor:
        sql = 'INSERT INTO ' \
              'cities (name_heb) ' \
              'VALUES (%s);'
        cursor.execute(sql, data)


def insert_property(data, connection):
    """ insert record to properties table """
    with connection.cursor() as cursor:
        sql = 'INSERT INTO properties ' \
              '(website_id, property_type_id, ad_type_id, city_id, contact_id) ' \
              'VALUES (%s, %s, %s, %s, %s);'
        cursor.execute(sql, data)


def insert_property_details(data, connection):
    """ insert record to property_details table """
    with connection.cursor() as cursor:
        sql = 'INSERT INTO property_details ' \
              '(property_id, address, neighborhood, rooms, ' \
              'size_m2, floor_property, floors_in_building, ' \
              'description, entry_date, condo_fee, arnona, safe_room,' \
              ' balcony, storeroom, security_bars, air_conditioning, furniture,' \
              'accessibility, elevator, parking, roommates, pets, sun_boiler) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
              '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        cursor.execute(sql, data)


def insert_price(data, connection):
    """ insert record to prices table """
    with connection.cursor() as cursor:
        sql = 'INSERT INTO prices ' \
              '(property_id, date, price) ' \
              'VALUES (%s, %s, %s);'
        cursor.execute(sql, data)


def update_property(website_id, updates, connection):
    """ update record in properties table.
    Use case example: property was announced as 'regular_apartment' and ad was corrected to 'penthouse' """
    with connection.cursor() as cursor:
        sql_head = 'UPDATE properties SET '
        sql_middle = ','.join([f'{column} = {value}' for column, value in updates.items()])
        sql_tail = f' WHERE website_id = {website_id}'
        cursor.execute("".join([sql_head, sql_middle, sql_tail]))


def update_property_details(property_id, updates, connection):
    """ update record in properties table.
    Use case example: property details were edited, description was corrected, etc. """
    with connection.cursor() as cursor:
        sql_head = 'UPDATE property_details SET '
        sql_middle = ','.join([f'{column} = {value}' for column, value in updates.items()])
        sql_tail = f' WHERE property_id = {property_id}'
        cursor.execute("".join([sql_head, sql_middle, sql_tail]))


def get_contact_id_foreign_key(result, connection, t):
    """
    get contact id foreign key. Take result, connection obj and current transaction count.
    if announcer is real estate agent, check if she is in contacts table. If not, insert.
    if announcer is private individual, we do not have access to details,
    so there is one preloaded record in contacts table.
    :param result: dictionary with the scraping result for a single ad
    :param connection: connection instance
    :param t: current transaction count
    :return: contact_id, updated t
    """

    if result['contact_type'] == 'מתיווך':
        if not query_db(f'SELECT id FROM contacts WHERE website_id = {result["contact_website_id"]}', connection):
            data = (result["contact_website_id"], result["contact_type"],
                    result["contact_office"], result["contact_name"], result["contact_phone"])
            insert_contact(data, connection)
            t += 1
        contact_id = query_db(f'SELECT id FROM contacts WHERE website_id = '
                              f'{result["contact_website_id"]}', connection)[0]['id']
    else:

        contact_id = query_db(f'SELECT id FROM contacts WHERE contact_type = "מפרטי" ', connection)[0]['id']

    return contact_id, t


def get_city_id_foreign_key(result, connection, t):
    """
    get city id foreign key.
    If city not in cities table, insert.
    :param result: dictionary with the scraping result for a single ad
    :param connection: connection instance
    :param t: current transaction count
    :return: city_id, updated t
    """
    if not query_db(f'SELECT * FROM cities WHERE name_heb = "{result["city"]}"', connection):
        insert_city((result["city"]), connection)
        t += 1
    city_id = query_db(f'SELECT * FROM cities WHERE name_heb = "{result["city"]}"', connection)[0]['id']

    return city_id, t


def insert_new_ad(ad_id, result, connection, t):
    """
    insert a new ad into the database,
    checking foreign ads in auxiliary tables,
    inserting records to the relevant tables.
    :param ad_id: ad id in website
    :param result: dictionary with the scraping result for a single ad
    :param connection: connection object
    :param t: transaction count
    :return: updated transaction count
    """
    # deal with foreign keys before inserting main record.
    contact_id, t = get_contact_id_foreign_key(result, connection, t)

    # if new city, populate cities table with Hebrew name (site does not have English names)
    city_id, t = get_city_id_foreign_key(result, connection, t)

    website_id = int(ad_id)

    property_type_id = query_db(f'SELECT id FROM property_types WHERE website_id = '
                                f'{result["property_type"]}', connection)[0]['id']
    ad_type_id = query_db(f'SELECT id FROM ad_types WHERE website_id = {result["ad_type"]}', connection)[0]['id']

    # now that we have all foreign keys, insert property record to properties table
    insert_property((website_id, property_type_id, ad_type_id, city_id, contact_id), connection)
    t += 1

    # property details. Deal with foreign keys and aux tables in the order displayed in the ERD.
    # get property_id
    property_id = query_db(f'SELECT id FROM properties WHERE website_id = {website_id}', connection)[0]['id']
    # arnona and condo_fee are integers, so preprocess.
    arnona = int(result['arnona']) if result['arnona'] else None
    condo_fee = int(result['condo_fee']) if result['condo_fee'] else None
    # get list with all details in order.
    data = [property_id,
            result['address'],
            result['neighborhood'],
            float(result['rooms']),
            int(result['size_m2']),
            result['floor_property'],
            result['floors_total'],
            result['description'][:1000],
            result['entry_date'],
            condo_fee,
            arnona,
            result['mamad'],
            result['mirpeset'],
            result['mahsan'],
            result['soragim'],
            result['mizug'],
            result['riut'],
            result['gisha'],
            result['maalit'],
            result['hania'],
            result['shutafim'],
            result['pets'],
            result['boiler']
            ]
    # insert property details record.
    insert_property_details(data, connection)
    t += 1

    # insert dated price record in separate table
    data = [property_id, result['date'], result['price']]
    insert_price(data, connection)
    t += 1

    return t


def update_current_add(website_id, result, connection, t):
    """
    for ad that is already in database, check previous record,
    and update only new info.
    :param website_id: ad id in website
    :param result: dictionary with the scraping result for a single ad
    :param connection: connection object
    :param t: transaction count
    :return: updated transaction count
    """

    # table properties
    # previous record
    prev_record = query_db(f'SELECT * FROM properties WHERE website_id = {website_id}', connection)[0]

    # new record
    property_type_id = query_db(f'SELECT id FROM property_types WHERE website_id = '
                                f'{result["property_type"]}', connection)[0]['id']
    ad_type_id = query_db(f'SELECT id FROM ad_types WHERE website_id = {result["ad_type"]}', connection)[0]['id']

    contact_id, t = get_contact_id_foreign_key(result, connection, t)

    city_id, t = get_city_id_foreign_key(result, connection, t)

    new_values = {'property_type_id': property_type_id,
                  'ad_type_id': ad_type_id,
                  'city_id': city_id,
                  'contact_id': contact_id}

    updates = {k: v for k, v in new_values.items() if v != prev_record[k]}

    if updates:
        update_property(website_id, updates, connection)
        t += 1

    # table property_details
    # previous record
    property_id = prev_record['id']  # from table properties
    prev_record = query_db(f'SELECT * FROM property_details WHERE property_id = {property_id}', connection)[0]

    # arnona and condo_fee are integers, so preprocess.
    arnona = int(result['arnona']) if result['arnona'] else None
    condo_fee = int(result['condo_fee']) if result['condo_fee'] else None

    new_values = {'address': result['address'],
                  'neighborhood': result['neighborhood'],
                  'rooms': float(result['rooms']),
                  'size_m2': int(result['size_m2']),
                  'floor_property': result['floor_property'],
                  'floors_in_building': result['floors_total'],
                  'description': result['description'][:1000],
                  'entry_date': result['entry_date'],
                  'condo_fee': condo_fee,
                  'arnona': arnona,
                  'safe_room': result['mamad'],
                  'balcony': result['mirpeset'],
                  'storeroom': result['mahsan'],
                  'security_bars': result['soragim'],
                  'air_conditioning': result['mizug'],
                  'furniture': result['riut'],
                  'accessibility': result['gisha'],
                  'elevator': result['maalit'],
                  'parking': result['hania'],
                  'roommates': result['shutafim'],
                  'pets': result['pets'],
                  'sun_boiler': result['boiler']
                  }

    updates = {k: v for k, v in new_values.items() if v != prev_record[k]}

    if updates:
        update_property_details(property_id, updates, connection)
        t += 1

    # prices are always considered a new record,
    # even if scraped twice in the same day (could have changed)
    data = [property_id, result['date'], result['price']]
    insert_price(data, connection)
    t += 1

    return t
