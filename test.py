import geopandas as gpd
# import osmnx as ox
from shapely.geometry import Point, Polygon, MultiPolygon
x = gpd.read_file("../Data/maz.geojson")
# print(x.head())
# kwargs = {'encoding': "ISO-8859-1"}
# tmp = gpd.read_file('../Data/gz_2010_us_050_00_5m.json', **kwargs)
# tmp_w_update_crs = tmp.to_crs({'init': 'epsg:2223'})
# print(tmp_w_update_crs.head())
# y = list()

rows = x["geometry"].iloc[0:-1]
# for index in range(0, len(rows)):
#     print(rows[index])
# new_rows = list()
# for row in rows:
# new_rows.append(MultiPolygon([row]))
# print(new_rows[0:5])
#     y.append(ox.quadrat_cut_geometry(row.bounds, quadrat_width=1))
