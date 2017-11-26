from shapely.geometry import shape, asShape
import shapely
import pymysql
import getpass
import json
import sqlite3 as sql

class apn_verification(object):
    def __init__(self):
        print('APN Verification started')


    def parsing_apns(self, filepath, database):
        # host, user, password, database, table_name, drop
        maz_file = open(filepath['maz'], 'r')
        maz_set = json.load(maz_file)
        maz_file.close()
        print("Maz Loaded")

        parcel_file = open(filepath['parcel'], 'r')
        parcel_set = json.load(parcel_file)
        parcel_file.close()
        print("Parcel Loaded")
        try:
            print("System Level DB created")
            conn = pymysql.connect(host=database['host'], user=database['user'], password=database['password'], database=database['database'])
        except KeyError as keyErr:
            print("File Level DB created")
            conn = sql.connect(database['database'])
        cur = conn.cursor()
        if (database['drop'] == True):
            cur.execute(("DROP TABLE if exists {}").format(database['table_name']))
            exec_str = ("CREATE table {0} (apn CHAR(9), maz INT UNSIGNED NOT NULL)").format(database['table_name'])
            cur.execute(exec_str)
        # else:
        #     if (database['drop'] == True):
        #         cur.execute(("DROP TABLE {0};").format(database['table_name']))
        #         exec_str = ("CREATE table {0} (apn INT UNSIGNED NOT NULL PRIMARY KEY, maz INT UNSIGNED NOT NULL, usage TINYINT UNSIGNED NOT NULL)").format(database['table_name'])
        #         cur.execute(exec_str)
        insert_tuple = list()
        progress = 0
        total = len(parcel_set['features'])
        for feature in parcel_set['features']:
            try:
                feature_shape = (shape(feature['geometry'])).representative_point()
            except ValueError as topExcep:
                feature_shape = shape(feature['geometry'])
            if (progress >= (.025*total)):print(".25");conn.commit()
            if (progress >= (.1*total)):print("1");conn.commit()
            if (progress >= (.2*total)):print("2");conn.commit()
            if (progress >= (.3*total)):print("3");conn.commit()
            if (progress >= (.4*total)):print("4");conn.commit()
            if (progress >= (.5*total)):print("5");conn.commit()
            if (progress >= (.6*total)):print("6");conn.commit()
            if (progress >= (.7*total)):print("7");conn.commit()
            if (progress >= (.8*total)):print("8");conn.commit()
            if (progress >= (.9*total)):print("9");conn.commit()
            progress += 1
            for bounding in maz_set['features']:
                bounding_shape = shape(bounding['geometry'])
                if feature_shape.within(bounding_shape):
                    insert_tuple = tuple([feature['properties']['APN'], bounding['properties']['MAZ_ID_10']])
                    print(insert_tuple)
                    exec_str = ("INSERT into {0} values {1};").format(database['table_name'], insert_tuple)
                    cur.execute(exec_str)
                    conn.commit()
                    # parcel_set['features'].remove(feature)
                    break
        conn.commit()
        conn.close()

if __name__ == "__main__":
    x = apn_verification()
    files = {'parcel': 'Parcels_All/all_parcel.geojson', 'maz':'real_maz/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'user':'root', 'password':pw, 'database':'apn', 'table_name':'apn_test', 'drop':True, 'host':'localhost'}
    x.parsing_apns(files, db_param)

