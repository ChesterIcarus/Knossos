from shapely.geometry import shape, polygon
from __init__ import bounding_for_maz
import json
import pyproj
import shapely
import pymysql
import getpass
import sqlite3 as sql
from functools import partial
from shapely.ops import transform
import xml.etree.ElementTree as xml


class apn_linking(object):
    def __init__(self):
        print('APN Linking started')

    def load_maz_and_parcel(self, filepath):
        # Loading big data for rest of class data
        maz_file = open(filepath['maz'], 'r')
        self.maz_set = json.load(maz_file)
        maz_file.close()
        print("Maz Loaded")

        parcel_file = open(filepath['parcel'], 'r')
        self.parcel_set = json.load(parcel_file)
        parcel_file.close()
        print("Parcel Loaded")
        try:
            self.crs = (self.parcel_set['crs']['properties']['name']).split(':')[-1]
        except (KeyError, IndexError) as geojson_err:
            print("Invalid GeoJSON for Parcel Set")
            exit()

    def get_valid_ways(self, osm_file):
        # Finding valid paths, requires *.osm file as input
        self.valid_way_dict = dict()
        root = (xml.parse(osm_file)).getroot()
        links = root[1]

        for link in links:
            _id = link.attrib['id'].split("_")[0]
            self.valid_way_dict[_id] = link.attrib['id']
            self.valid_way_dict[link.attrib['from']] = link.attrib['id']

    def db_connection(self, database):
        # Connection to database for output
        try:
            self.conn = pymysql.connect(host=database['host'], user=database['user'], password=database['password'], database=database['database'])
            print("System Level DB created")
        except KeyError as keyErr:
            self.conn = sql.connect(database['database'])
            print("File Level DB created")
        self.cur = self.conn.cursor()
        if (database['drop'] is True):
            self.cur.execute(("DROP TABLE if exists {}").format(database['table_name']))
            exec_str = ("CREATE table {0} (osm_way_id CHAR(12), APN CHAR(12), maz INT UNSIGNED NOT NULL)").format(database['table_name'])
            self.cur.execute(exec_str)
        self.table_name = database['table_name']

    def apn_bounding(self, apn_bounding=None):
        # Setting bounding on the map for reduced APN eval. based on MAZ
        if (apn_bounding is not None):
            bounding_from_inp = polygon.Polygon(apn_bounding['poly_coords'], apn_bounding['poly_holes'] if ('poly_holes' in apn_bounding.keys()) else None)
            proj_to_map = partial(pyproj.transform, pyproj.Proj(init=apn_bounding['poly_crs']), pyproj.Proj(init=('epsg:{}').format(self.crs)))
            bounding_for_maz = transform(proj_to_map, bounding_from_inp)
            
        else:
            default_bounding = polygon.Polygon([(649054, 896498), (649192, 888749), (665535, 896129), (663998, 889026)])
            proj_to_map = partial(pyproj.transform, pyproj.Proj(init='epsg:2223'), pyproj.Proj(init=('epsg:{}').format(self.crs)))
            bounding_for_maz = transform(proj_to_map, default_bounding)
        self.bounding_for_maz = bounding_for_maz
        return bounding_for_maz

    def apn_MAZ_osmID_linking(self, db_output, bounding_for_maz=None):
        # "Meat" of the module, connection MAZ, APN, and osm_id
        # This creates the output to be used in agent plan generation
        if (bounding_for_maz is None):
            try:
                bounding_for_maz = self.bounding_for_maz
            except AttributeError as ex:
                raise ex
        self.osm_apn_maz = dict()
        insert_tuple = list()
        progress = 0
        total = len(self.parcel_set['features'])
        for maz in self.maz_set['features']:
            try:
                if not ((shape(maz['geometry'])).representative_point()).within(bounding_for_maz):
                    self.maz_set['features'].remove(maz)
            except ValueError as topErr:
                if not (shape(maz['geometry'])).within(bounding_for_maz):
                    self.maz_set['features'].remove(maz)
        for feature in self.parcel_set['features']:
            try:
                feature_shape = (shape(feature['geometry'])).representative_point()
            except (TypeError, ValueError) as topExcep:
                try:
                    feature_shape = shape(feature['geometry'])
                except TypeError as te:
                    pass
            progress += 1
            if feature_shape.within(bounding_for_maz):
                for bounding in self.maz_set['features']:
                    bounding_shape = shape(bounding['geometry'])
                    if feature_shape.within(bounding_shape):
                        if feature['properties']['osm_id'] in self.valid_way_dict:
                            insert_tuple = tuple([self.valid_way_dict[feature['properties']['osm_id']], 
                                feature['properties']['APN'], bounding['properties']['MAZ_ID_10']])
                            if (db_output is True):
                                exec_str = ("INSERT INTO {} values {};").format(self.table_name, insert_tuple)
                                self.cur.execute(exec_str)
                            else:
                                if (bounding['properties']['MAZ_ID_10'] in self.osm_apn_maz):
                                    self.osm_apn_maz[bounding['properties']['MAZ_ID_10']].append(bounding)
                                else:
                                    self.osm_apn_maz[bounding['properties']['MAZ_ID_10']] = [list(insert_tuple)]
        if (db_output is True):
            self.conn.commit()
            self.conn.close()

if __name__ == "__main__":
    # x = apn_linking()
    files = {'parcel': '../Shapefiles/Cleaned/test_dirty_point.geojson', 'maz': 'real_maz/min_maz.geojson', 'osm': '../Shapefiles/Cleaned/working_test.xml'}
    # pw = getpass.getpass()
    # host, user, password, database, table_name, drop
    db_param = {'user':'root', 'database':'cleaned.db', 'table_name':'clean', 'drop':True, 'host':'localhost'}
    # x.parsing_apns(files, db_param)

