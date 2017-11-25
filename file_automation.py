import geopandas as gp
import os
import fiona
import pandas
# data = gp.read_file("output.shp")
# print(data.head())
# print(data.bounds)

class file_automation(object):

    def __init__(self):
        print("File Automation module started.")
        self.geometry_list = dict()


    def poly_to_centroid(self, in_file, out_file, store_in_ram, write_to_file, crs_= None):
        data = gp.read_file(in_file)
        print("?")
        # if (crs_ != None):
        #     data.to_crs(crs_)
        X = data.representative_point()
        print(X.head(10))
        print("Here")
        data['representative_point'] = X
        print(data.head(10))
        if (store_in_ram == True):
            # q = gp.geodataframe.GeoSeries(X.geometry)
            self.main_file = data
        if (write_to_file == True):
            X.to_file(out_file)
            # print("")
    

    def spatial_join(self, from_file, out_file, file_list=None):
        # if (from_file == False):
        #     self.spatial_iter(True, out_file)
        if from_file == True:
            for file_ in file_list:
                j = gp.read_file(file_['filepath'])
                j.to_file(out_file, layer=file_['args']['layer'])
        

        #     x = pandas.DataFrame(self.geometry_list[key])
        #     list_to_merge.append(x)
        # current_merged = pandas.concat(list_to_merge)
        
        # if file_write == True:
        #     z = gp.GeoDataFrame(current_merged)
        #     with fiona.open(out_file) as file_:
        #         with fiona.open
        # else:
        #     return current_merged
                


if (__name__ == "__main__"):
    x = file_automation()
    x.poly_to_centroid("real_maz/MAZ_10_TAZ_2015.shp", "pleWork.shp", True, True)
    file_list_ = [{'args':{'layer':"pleWork", 'filepath':"pleWork.shp"}, 'filepath':'pleWork.shp'}]
    x.spatial_join(True, "pleWork.shp", file_list_)