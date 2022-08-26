"""
script to create Mysql database for Real Estate scraper project.
"""


import pymysql.cursors
import config
import configparser

import updatedb


def create_db(connection):
    with connection.cursor() as cursor:
        sql = []
        if not updatedb.query_db(f"SHOW DATABASES LIKE '{config.DB_NAME}' ", connection):
            sql.append(f'CREATE DATABASE {config.DB_NAME};')
        sql.append(f'USE {config.DB_NAME};')
        sql.append('CREATE TABLE properties ( \
                    id int PRIMARY KEY AUTO_INCREMENT, \
                    website_id int, \
                    property_type_id int,\
                    ad_type_id int, \
                    city_id int, \
                    contact_id int \
                    );')
        sql.append('CREATE TABLE property_types ( \
                    id int PRIMARY KEY AUTO_INCREMENT,\
                    website_id int,\
                    name varchar(255)\
                    );')
        sql.append('CREATE TABLE ad_types (\
                    id int PRIMARY KEY AUTO_INCREMENT,\
                    website_id int,\
                    name varchar(255)\
                    );')
        sql.append('CREATE TABLE cities (\
                    id int PRIMARY KEY AUTO_INCREMENT,\
                    name_heb varchar(255),\
                    name_eng varchar(255)\
                    );')
        sql.append('CREATE TABLE contacts (\
                    id int PRIMARY KEY AUTO_INCREMENT,\
                    website_id int,\
                    contact_type varchar(255),\
                    office varchar(255),\
                    name varchar(255),\
                    phone int\
                    );')
        sql.append('CREATE TABLE property_details (\
                    property_id int PRIMARY KEY,\
                    address varchar(255),\
                    neighborhood varchar(255),\
                    rooms float,\
                    size_m2 int,\
                    floor_property varchar(255),\
                    floors_in_building varchar(255),\
                    description varchar(1000),\
                    entry_date varchar(255),\
                    condo_fee int,\
                    arnona int,\
                    safe_room boolean,\
                    balcony boolean,\
                    storeroom boolean,\
                    security_bars boolean,\
                    air_conditioning boolean,\
                    furniture boolean,\
                    accessibility boolean,\
                    elevator boolean,\
                    parking boolean,\
                    roommates boolean,\
                    pets boolean,\
                    sun_boiler boolean\
                    );')
        sql.append('CREATE TABLE prices (\
                    id int PRIMARY KEY AUTO_INCREMENT,\
                    property_id int,\
                    date date,\
                    price int\
                    );')
        sql.append('CREATE TABLE demographics (\
                    city_id int PRIMARY KEY,\
                    total_pop int,\
                    age_0_5 int,\
                    age_6_18 int,\
                    age_19_45 int,\
                    age_46_55 int,\
                    age_56_64 int,\
                    age_65_plus int\
                    );')
        sql.append('ALTER TABLE properties ADD FOREIGN KEY (property_type_id) REFERENCES property_types (id);')
        sql.append('ALTER TABLE properties ADD FOREIGN KEY (ad_type_id) REFERENCES ad_types (id);')
        sql.append('ALTER TABLE properties ADD FOREIGN KEY (city_id) REFERENCES cities (id);')
        sql.append('ALTER TABLE properties ADD FOREIGN KEY (contact_id) REFERENCES contacts (id);')
        sql.append('ALTER TABLE property_details ADD FOREIGN KEY (property_id) REFERENCES properties (id);')
        sql.append('ALTER TABLE prices ADD FOREIGN KEY (property_id) REFERENCES properties (id);')
        sql.append('ALTER TABLE demographics ADD FOREIGN KEY (city_id) REFERENCES cities (id);')
        for command in sql:
            cursor.execute(command)
        connection.commit()

# populate property_types, ad_types, cities.
# Insert 'private announcer' in contact table.
# If the contact_type is 'private individual', we do not have access to contact details.
# So we keep only one record for all 'private individual' announcers, as there would be
# no benefit in creating a new record for each ad whose announcer is private.


def preload_db(connection):
    with connection.cursor() as cursor:
        # property_types
        sql = 'INSERT INTO property_types (website_id, name) VALUES (%s, %s);'
        data = [(prop_id, name) for prop_id, name in config.PROPERTY_TYPES.items()]
        cursor.executemany(sql, data)
        # ad_types
        sql = 'INSERT INTO ad_types (website_id, name) VALUES (%s, %s);'
        data = [(ad_id, name) for ad_id, name in config.AD_TYPES.items()]
        cursor.executemany(sql, data)
        # cities
        sql = 'INSERT INTO cities (name_eng, name_heb) VALUES (%s, %s);'
        data = list(config.CITIES.items())
        cursor.executemany(sql, data)
        # contact: private individual
        sql = 'INSERT INTO contacts (contact_type) VALUES (%s);'
        data = 'מפרטי'
        cursor.execute(sql, data)
        connection.commit()


def main():
    cred = configparser.ConfigParser()
    cred.read('credentials.ini')
    connection = pymysql.connect(host=cred['DB']['host'],
                                 user=cred['DB']['user'],
                                 password=cred['DB']['password'],
                                 cursorclass=pymysql.cursors.DictCursor)
    create_db(connection)
    preload_db(connection)
    connection.close()


if __name__ == '__main__':
    main()
