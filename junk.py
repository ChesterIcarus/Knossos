import json
from shapely.geometry import shape, polygon


f1_ = open('./Parcels_All/all_parcel.geojson', 'r')
x1 = json.load(f1_)
f1_.close()

x2 = x1
for index,item in enumerate(x1['features']):
    try:
        rep = (shape(item['geometry'])).representative_point()
        try:
            x2['features'][index]['geometry']['coordinates'] = rep.coords[0]
        except IndexError as idxErr:
            x2['features'][index]['geometry']['coordinates'] = rep.coords
        x2['features'][index]['geometry']['type'] = "Point"
    except ValueError as val:
        print("qwe")
    del x2['features'][index]['properties']['DBT_BEGIN']
    del x2['features'][index]['properties']['BEGIN_DATE']
    del x2['features'][index]['properties']['Shape_Leng']
    del x2['features'][index]['properties']['FLOOR']
    del x2['features'][index]['properties']['ADDRESS']
    del x2['features'][index]['properties']['AREA']
    del x2['features'][index]['properties']['Shape_Area']

f2_ = open('./Parcels_All/point_parcel.geojson', 'w')
json.dump(x2, f2_)
f2_.close()