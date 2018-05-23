import json
import pyproj
import getpass
import pymysql as sql
from functools import partial
# from shapely.wkt import load
import shapely.wkt as wkt
from shapely.ops import transform
from shapely.geometry import shape, polygon, LineString


class LinkingApnToMaz:
    def __init__(self):
        print('APN\'s linked to MAZ started (NOT USING OSMID\'s)')
        self.apn_maz = dict()
        self.maz_set = None
        self.parcel_set = None
        self.crs = None
        self.conn = None
        self.cur = None
        self.db_name = None
        self.table_name = None
        self.bounding_for_maz = None
        self.bounded_maz_set = None
        self.bounded_maz_shapes = list()
        self.bounded_eval = False
        self.db_insert = list()

    # Loading maz data for rest of class data
    def load_maz(self, filepath):
        maz_file = open(filepath, 'r')
        self.maz_set = json.load(maz_file)
        maz_file.close()
        print("Maz Loaded")

    # Loading parcel data for rest of class data
    def load_parcel(self, filepath):
        parcel_file = open(filepath, 'r')
        self.parcel_set = json.load(parcel_file)
        parcel_file.close()

    def set_crs_from_parcel(self):
        try:
            self.crs = (self.parcel_set['crs']
                        ['properties']['name']).split(':')[-1]
        except (KeyError, IndexError) as err:
            print("Invalid GeoJSON for Parcel Set")
            raise err
        print("Parcel Loaded with CRS:" + str(self.crs))

    def connect_database(self, database, table_name, drop):
        '''Connecting to Database used to store the output, if the output is to be written to a database.
        Input is of the form: database =
            {'host': port, 'user': Valid Database User,
            'password': Password for the specified user, 'db': Database to use},
            table_name =  Name of the table to use (Must exist if drop is False),
            drop =  Boolean indicating if we are to drop existing table='tablename
        Output is stored in the form: (X Coordinate, Y Coordinate, APN Identifier, MAZ Identifier)'''

        self.table_name = table_name
        self.db_name = database['db']
        try:
            # self.conn = pymysql.connect(host='localhost',user='root',
            #       password='Am1chne>', db='linkingApnToMaz')
            self.conn = sql.connect(**database)
            print("System Level DB created")
        except KeyError:
            self.conn = sql.connect(db=database['database'])
            print("File Level DB created")
        self.cur = self.conn.cursor()
        if drop is True:
            self.cur.execute(("DROP TABLE if exists {}").format(table_name))
            exec_str = ("CREATE table {0} (coordX FLOAT, coordY FLOAT, APN CHAR(12), maz INT UNSIGNED NOT NULL)").format(
                table_name)
            self.cur.execute(exec_str)
            self.conn.commit()

    def set_bounding(self, wkt_filepath=None, wkt_crs=None):
        '''Allows the user to specify a subsection of the entered area to evaluate'''
        # Setting bounding on the map for reduced APN eval. based on MAZ
        if (wkt_filepath is not None) and (wkt_crs is not None):
            wkt_text = str
            with open(wkt_filepath, 'r') as handle:
                wkt_text = handle.read()
            bounding_from_inp = wkt.loads(wkt_text)
            proj_to_map = partial(pyproj.transform, pyproj.Proj(
                init=wkt_crs), pyproj.Proj(init=('epsg:{}').format(self.crs)))
            bounding_for_maz = transform(proj_to_map, bounding_from_inp)

        else:
            default_bounding = polygon.Polygon(
                [(649054, 896498), (649192, 888749), (665535, 896129), (663998, 889026)])
            proj_to_map = partial(pyproj.transform, pyproj.Proj(
                init='epsg:2223'), pyproj.Proj(init=('epsg:{}').format(self.crs)))
            bounding_for_maz = transform(proj_to_map, default_bounding)
        self.bounding_for_maz = bounding_for_maz

        print("Boundries set!")
        return bounding_for_maz
        # for index, point in enumerate(data['geometry'][0]['coordinates']):
        #     data['geometry'][0]['coordinates'][index] = pyproj.transform(epsg_4326, epsg_2223, point[0], point[1])

        # self.bounding_for_maz = shape(data['geometry'][0])

    def find_maz_in_bounds(self):
        print("Finding MAZ in bounds")
        self.bounded_maz_set = {'features': list()}
        print(
            f"There are {len(self.maz_set['features'])} MAZ\'s before boundry operations")
        for maz in self.maz_set['features']:
            temp_shape = shape(maz['geometry'])
            temp_point = temp_shape.representative_point()
            if temp_point.within(self.bounding_for_maz):
                self.bounded_maz_set['features'].append(maz)
        print(
            f"Found {len(self.bounded_maz_set['features'])} MAZ\'s in bounds")

    def create_maz_shape_list(self, maz_list):
        ret_list = list()
        for maz in maz_list['features']:
            ret_list.append(
                tuple([shape(maz['geometry']), maz['properties']['MAZ_ID_10']]))
        return ret_list

    def assign_maz_per_apn(self, write_to_database=False):
        # "Meat" of the module, connection MAZ, APN, and osm_id
        # This creates the output to be used in agent plan generation
        print("Assigning MAZ per APN")
        print(f"There are {len(self.parcel_set['features'])} total features")
        maz_shape_list = self.create_maz_shape_list(self.bounded_maz_set)
        print(f"There are {len(maz_shape_list)} MAZ\'s")

        for feature in self.parcel_set['features']:
            temp_shape = shape(feature['geometry'])
            try:
                temp_point = temp_shape.representative_point()
            except (TypeError, ValueError):
                temp_point = temp_shape
            if temp_point.within(self.bounding_for_maz):
                for maz in maz_shape_list:
                    if temp_point.within(maz[0]):
                        self.db_insert.append(tuple([temp_point.x,
                                                     temp_point.y,
                                                     feature['properties']['APN'],
                                                     maz[1]]))

        if write_to_database is True:
            print("Writing to database")
            print(len(self.db_insert))
            self.cur.executemany(
                f"INSERT INTO {self.db_name}.{self.table_name} values (%s,%s,%s,%s)", self.db_insert)
            self.conn.commit()
            self.conn.close()
        print("MAZ\'s assigned")


if __name__ == "__main__":
    files = {'parcel': '../Data/parcel.geojson', 'maz': '../Data/maz.geojson'}
    pw = getpass.getpass()
    db_param = {'user': 'root', 'db': 'linkingApnToMaz',
                'host': 'localhost', 'password': pw}

    example = LinkingApnToMaz()
    example.connect_database(db_param, table_name="FullArizona", drop=True)
    example.load_maz(files['maz'])
    example.load_parcel(files['parcel'])
    example.set_crs_from_parcel()
    example.set_bounding(wkt_filepath='maricopa_poly.wkt', wkt_crs='epsg:4326')
    # example.set_bounding()
    example.find_maz_in_bounds()
    example.assign_maz_per_apn(write_to_database=True)
