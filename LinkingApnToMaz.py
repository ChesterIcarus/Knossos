import json
import sys
import geojson
import pyproj
import getpass
import rtree
import pandas as pd
import pymysql as sql
from collections import defaultdict
import tables as tb
from functools import partial
import osmnx as ox
import geopandas as gpd
from shapely.ops import transform
from shapely.geometry import shape, polygon, LineString, mapping, MultiPolygon
import shapely
from multiprocessing import Pool

MAZ_DF = gpd.GeoDataFrame()
MULTITHREAD = True
POOLSIZE = 8
# MAZ_GEOMETRY = gpd.GeoDataFrame()
MAZ_SLICES = defaultdict(list)


def multiproc_maz_apn_assoc(parcel_set):
    ret_list = list()
    pause_on_execp = True

    for parcel in parcel_set:
        parcel_shape = shape(parcel['geometry'])
        try:
            parcel_point = parcel_shape.representative_point()
        except Exception:
            parcel_point = parcel_shape
        try:
            for index, row in enumerate(MAZ_DF.iloc['MAZ'][0:-1]):
                if MAZ_DF["geometry"][index].contains(parcel_point):
                    ret_list.append(tuple([parcel_point.x,
                                           parcel_point.y,
                                           parcel_point['properties']['APN'], row["MAZ"]]))
        except Exception as general_ex:
            if pause_on_execp:
                print(general_ex)
                resp = input(
                    'Pause for future exceptions? y/n ("y" if you would like the option to write the current data to a JSON file)\n')
                if resp == "n":
                    pause_on_execp = False
                else:
                    prompt_geojson_dump(
                        ret_list, "ret_list", shape=parcel_shape, point=parcel_point)
    return ret_list


def prompt_geojson_dump(data, data_name, point=None, shape=None):
    resp = input(
        f"Would you like to write the data for {data_name} to a JSON file? y/n\n")
    if resp == 'y':
        f_name = input(
            "Please enter entire filename (eg. 'test.json')\n")
        with open(f_name, 'w+') as handle:
            try:
                geojson.dump({
                    "db_insert": data,
                    "temp_point": point if (point is not None) else None,
                    "temp_shape": shape if (shape is not None) else None},
                    handle)
            except Exception as file_exep:
                print(
                    f'Unable to write to file due to:\n{file_exep}')


class LinkingApnToMaz:
    def __init__(self):
        print('APN\'s linked to MAZ started (NOT USING OSMID\'s)')
        global POOLSIZE
        global MULTITHREAD
        self.USING_SQL = False
        self.USING_PYTABLES = False
        self.apn_maz = dict()
        self.maz_set = None
        self.parcel_set = None
        self.crs = None
        self.conn = None
        self.cur = None
        self.h5f = None
        self.h5f_table_name = None
        self.db_name = None
        self.table_name = None
        self.bounding_for_maz = None
        self.bounded_maz_df = gpd.GeoDataFrame()
        self.bounded_eval = False
        self.db_insert = list()
        self.spatial_index = rtree.index.Index()

    # Loading maz data for rest of class data
    def load_maz(self, filepath):
        self.maz_set = gpd.read_file(filepath)

    # Loading parcel data for rest of class data
    def load_parcel(self, filepath):
        self.parcel_set = gpd.read_file(filepath)

    def set_crs_from_parcel(self):
        self.crs = '2223'
        print("Parcel Loaded with CRS:" + str(self.crs))

    def connect_PyTable(self, filepath, table_name='example', h5f_description=None):
        self.h5f = tb.open_file(filepath, 'w')
        self.h5f_table_name = table_name
        if h5f_description is None:
            tmp_desc = {'coordX': tb.FloatCol(),
                        'coordY': tb.FloatCol(),
                        'APN': tb.StringCol(12),
                        'maz': tb.Int32Col()}
        tbl = self.h5f.create_table('/', self.h5f_table_name, tmp_desc)
        self.USING_PYTABLES = True

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
        self.USING_SQL = True

    def set_bounding(self, geojson_filepath=None, geojson_crs=None):
        '''Allows the user to specify a subsection of the area to evaluate.
            This subsection will be identified by MAZ ID's.'''
        if (geojson_filepath is not None) and (geojson_crs is not None):
            # tmp = gpd.read_file('../Data/gz_2010_us_050_00_5m.geojson')
            tmp = gpd.read_file(geojson_filepath)
            tmp.crs = {'init': geojson_crs}
            tmp_w_update_crs = tmp.to_crs({'init': 'epsg:2223'})
            for index, name in enumerate(tmp_w_update_crs['NAME']):
                if name == "Maricopa":
                    self.bounding_for_maz = gpd.GeoSeries(
                        tmp_w_update_crs['geometry'][index])[0]

        else:
            default_obj = {
                'type': 'Polygon',
                'coordinates': [[[649054, 896498], [649192, 888749], [665535, 896129], [663998, 889026]]]}
            default_shp = shapely.geometry.shape(default_obj)
            tmp = gpd.GeoSeries([default_shp])
            tmp.crs = {'init': 'epsg:2223'}
            bounding_for_maz = tmp.to_crs({'init': f'epsg:{self.crs}'})[0]
            df = pd.DataFrame({'bound': [1]})
            self.bounding_for_maz = gpd.GeoDataFrame(
                df, geometry=[bounding_for_maz])

        print("Boundries set!")
        # return bounding_for_maz

    def find_maz_in_bounds(self):
        self.bounded_maz_df = gpd.sjoin(
            self.maz_set, self.bounding_for_maz, how='left')
        MAZ_DF = self.bounded_maz_df
        print(
            f"Found {len(self.bounded_maz_df['geometry'])} MAZ\'s in bounds")

    def assign_maz_per_apn(self, write_to_database=False, write_to_h5f=False, write_json=False, json_path="MAZ_by_APN.json", maz_bounds_read=False, maz_bounds_json=False, maz_bounds_path="valid_maz_for_bounds.json"):

        if not maz_bounds_read:
            self.find_maz_in_bounds()
        else:
            with open(maz_bounds_path, 'r') as handle:
                MAZ_DF = gpd.read_file(handle)
                MAZ_DF.crs = {'init': 'epsg:2223'}
        print(f"There are {len(self.parcel_set['features'])} total features")
        print(f"There are {len(MAZ_DF['geometry'])} total MAZ\'s")
        tmp_list = list()
        for index, row in enumerate(MAZ_DF['geometry'].iloc[0:-1]):
            tmp_list.append(tuple([MultiPolygon([row]), MAZ_DF['MAZ'][index]]))

        for row in tmp_list:
            tmp = ox.quadrat_cut_geometry(row[0], 1)
            MAZ_SLICES[row[1]].append(tmp)

        point_index = MAZ_DF.sindex
        points_within_geo = defaultdict(list)

        for maz in list(MAZ_SLICES):
            for maz_slice in maz:
                maz_sub = maz_slice.buffer(1e-14).buffer(0)
                possible_matches_index = list(
                    point_index.intersection(maz_sub.bounds))
                possible_matches = MAZ_DF['geometry'].iloc[possible_matches_index]
                precise_matches = possible_matches[possible_matches.intersects(
                    maz_sub)]

                points_within_geo[maz].append(precise_matches)
        # if MULTITHREAD == True:
        #     pool_size = POOLSIZE
        #     with Pool(pool_size) as pool:
        #         arg_list = list()
        #         self.db_insert = pool.map(multiproc_maz_apn_assoc,
        #                                   self.parcel_set['features'])
        db_insert = list()
        for maz in list(points_within_geo):
            for match in maz:
                print(match)
                input()
        if write_to_database:
            print(
                f"Writing {len(db_insert)} valid MAZ - APN relations to database")
            self.cur.executemany(
                f"INSERT INTO {self.db_name}.{self.table_name} values (%s,%s,%s,%s)", db_insert)
            self.conn.commit()
            self.conn.close()

        if write_to_h5f:
            new_row = (self.h5f.get_node('/', self.h5f_table_name)).row
            for row_ in db_insert:
                new_row['coordX'] = float(row_[0])
                new_row['coordY'] = float(row_[1])
                new_row['APN'] = str(row_[2])
                new_row['maz'] = row_[3]
                new_row.append()
            (self.h5f.get_node('/', self.h5f_table_name)).flush()
            (self.h5f.get_node('/', self.h5f_table_name)).close()

        if write_json:
            with open(json_path, 'w+') as handle:
                try:
                    json.dump(db_insert, handle)
                except Exception as excep_1:
                    try:
                        geojson.dump(db_insert, handle)
                    except Exception as excep_2:
                        print(
                            f"Unable to write final MAZ - APN relations to file: {json_path} due to:\n{excep_1}")
        print("MAZ - APN relations wrote to appropiate destinations")


if __name__ == "__main__":
    files = {'parcel': '../Data/parcel.geojson', 'maz': '../Data/maz.geojson'}
    # pw = getpass.getpass()
    # db_param = {'user': 'root', 'db': 'linkingApnToMaz',
    #             'host': 'localhost', 'password': 'Am1chne>'}
    example = LinkingApnToMaz()
    sys.stdout.flush()
    # example.connect_database(db_param, table_name="FullArizona", drop=True)
    example.connect_PyTable('h5f_example.hf')
    sys.stdout.flush()
    example.load_maz(files['maz'])
    sys.stdout.flush()
    example.load_parcel(files['parcel'])
    sys.stdout.flush()
    example.set_crs_from_parcel()
    sys.stdout.flush()
    example.set_bounding(
        geojson_filepath='../Data/gz_2010_us_050_00_5m.geojson', geojson_crs='epsg:4326')
    sys.stdout.flush()
    example.find_maz_in_bounds()
    sys.stdout.flush()
    # example.create_maz_shape_list(
    # example.bounded_maz_set, True, "valid_maz_for_bounds.json")
    example.assign_maz_per_apn(
        write_to_h5f=True, write_json=True, maz_bounds_json=True)
    sys.stdout.flush()
