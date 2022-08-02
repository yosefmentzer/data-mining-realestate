"""configuration file for Real Estate scraper"""

DEFAULT_TSIZE = 300

BOOLEAN_FEATURES = ['mamad', 'mirpeset', 'mahsan', 'soragim', 'mizug',
                    'riut', 'gisha', 'maalit', 'hania', 'shutafim',
                    'pets', 'boiler']

PROPERTY_TYPES = {1: 'regular_apartment',
                  2: 'two_family',
                  3: 'penthouse',
                  4: 'garden_apartment',
                  5: 'house',
                  6: 'duplex',
                  10: 'studio',
                  12: 'dwelling_unit'}

AD_TYPES = {1: 'rent',
            2: 'sale'}

CITIES = {'Jerusalem': 'ירושלים',
          'Tel Aviv Yaffo': 'תל אביב יפו',
          'Haifa': 'חיפה',
          'Bat Yam': 'בת ים',
          'Beer Sheva': 'באר שבע',
          'Netanya': 'נתניה',
          'Ashkelon': 'אשקלון',
          'Ashdod': 'אשדוד',
          'Lod': 'לוד',
          'Ramat Gan': 'רמת גן',
          'Petach Tikva': 'פתח תקווה',
          'Rechovot': 'רחובות',
          'Bnei Brak': 'בני ברק',
          'Holon': 'חולון',
          'Rishon Letzion': 'ראשון לציון',
          'Ramle': 'רמלה',
          'Tveria': 'טבריה',
          'Kfar Saba': 'כפר סבא',
          'Akko': 'עכו',
          'Hertzliya': 'הרצליה',
          'Hadera': 'חדרה',
          'Maale Adumim': 'מעלה אדומים',
          'Netivot': 'נתיבות',
          'Eilat': 'אילת',
          'Dimona': 'דימונה',
          'Nahariya': 'נהריה',
          'Kiryat Yam': 'קריית ים',
          'Modiin-Maccabim-Reut': 'מודיעין-מכבים-רעות*',
          'Kiryat Gat': 'קריית גת',
          'Or Akiva': 'אור עקיבא',
          'Kiryat Motzkin': 'קריית מוצקין',
          'Raanana': 'רעננה',
          'Hod Hasharon': 'הוד השרון',
          'Beit Shean': 'בית שאן',
          'Modiin Illit': 'מודיעין עילית',
          'Arad': 'ערד',
          'Beit Shemesh': 'בית שמש',
          'Nesher': 'נשר',
          'Nof Hagalil': 'נוף הגליל',
          'Givatayim': 'גבעתיים',
          'Afula': 'עפולה',
          'Kfar Yona': 'כפר יונה',
          'Mevaseret Zion': 'מבשרת ציון',
          'Ness Ziona': 'נס ציונה',
          'Ramat Hasharon': 'רמת השרון',
          'Yavne': 'יבנה',
          'Kiryat Shmona': 'קריית שמונה',
          'Kiryat Bialik': 'קריית ביאליק',
          'Harish': 'חריש',
          'Yehud-Monosson': 'יהוד-מונוסון',
          'Tzfat': 'צפת',
          'Kiryat Ata': 'קריית אתא',
          'Sderot': 'שדרות',
          'Givat Shmuel': 'גבעת שמואל',
          'Migdal Haemek': 'מגדל העמק',
          'Mazkeret Batya': 'מזכרת בתיה',
          'Kiryat Ono': 'קריית אונו',
          'Or Yehuda': 'אור יהודה',
          'Beer Yaakov': 'באר יעקב',
          'Yerucham': 'ירוחם',
          'Rosh Haayin': 'ראש העין',
          'Beitar Illit': 'ביתר עילית',
          'Givat Zeev': 'גבעת זאב',
          'Zichron Yaakov': 'זכרון יעקב',
          'Binyamina-Givat Ada': 'בנימינה-גבעת עדה*',
          'Avnei Hefetz': 'אבני חפץ',
          'Kiryat Arba': 'קריית ארבע',
          'Natzeret': 'נצרת',
          'Kiryat Malachi': 'קריית מלאכי',
          'Yokneam Illit': 'יקנעם עילית',
          'Hatzor Haglilit': 'חצור הגלילית',
          'Tirat Carmel': 'טירת כרמל',
          'Tzur Hadassah': 'צור הדסה',
          'Elad': 'אלעד',
          'Ofakim': 'אופקים',
          'Kadima Zoran': 'קדימה-צורן',
          'Ganei Tikva': 'גני תקווה',
          'Ariel': 'אריאל',
          'Immanuel': 'עמנואל',
          'Tzur Yitzhak': 'צור יצחק',
          'Efrat': 'אפרת',
          'Karmei Zur': 'כרמי צור',
          'Kedar': 'קדר',
          'Bet El': 'בית אל',
          'Pardes Hanna-Karkur': 'פרדס חנה-כרכור',
          'Katsrin': 'קצרין',
          'Shoresh': 'שורש',
          'Kiryat Ekron': 'קריית עקרון',
          'Mitzpe Ramon': 'מצפה רמון',
          'Rechasim': 'רכסים',
          'Midrach Oz': 'מדרך עוז',
          'Bnei Ayish': 'בני עייש',
          'Karmiel': 'כרמיאל',
          'Aluma': 'אלומה',
          'Caesarea': 'קיסריה',
          'Gedera': 'גדרה'}