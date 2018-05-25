import geopandas as gpd
# import osmnx as ox
from shapely.geometry import Point, Polygon, MultiPolygon
# x = gpd.read_file("../Data/maz.geojson")
# print(x.head())
# kwargs = {'encoding': "ISO-8859-1"}
tmp = gpd.read_file('../Data/gz_2010_us_050_00_5m.geojson')
tmp_w_update_crs = tmp.to_crs({'init': 'epsg:2223'})
for index, name in enumerate(tmp_w_update_crs['NAME']):
    if name == "Maricopa":
        x = gpd.GeoSeries(tmp_w_update_crs['geometry'][index])
        print(gpd.GeoSeries(tmp_w_update_crs['geometry'][index]))
        print(x[0])
# with open('../Data/gz_2010_us_050_00_5m.json', 'r', encoding='ISO-8859-1') as handle:
#     q = handle.read()
# with open('../Data/gz_2010_us_050_00_5m.geojson', 'w+') as handle:
#     handle.write(q)
