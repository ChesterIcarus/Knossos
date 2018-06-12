import json
from collections import defaultdict
from pyproj import Proj, transform


class TripPlanToActLegPlan:
    ''' This class is key for converting the MAG plans by agent, with assigned APN\'s
    into the format of ACT -> LEG -> ACT utilized by MATsim. '''

    def __init__(self):
        print(
            'Trip plan MAG format being converted into ACT -> LEG -> ACT\nformat for MATsim')
        self.projection = None
        self.trip_plan_from_file = list()
        self.actor_plans = list()

    def load_raw_plans(self, filepath):
        with open(filepath, 'r') as handle:
            self.trip_plan_from_file = json.load(handle)
        print(f'Actors plan in MAG format read from {filepath}')

    def set_projection(self, current_proj, new_proj):
        self.projection = [Proj(init=current_proj, preserve_units=True), Proj(
            init=new_proj, preserve_units=True)]

    def proj_coord(self, x, y):
        x_, y_ = transform(self.projection[0], self.projection[1], x=x, y=y)
        return x_, y_

    def plan_conversion(self, proj=False):
        for actor_id in list(self.trip_plan_from_file):
            self.actor_plans.append(dict())
            self.actor_plans[-1]["person_id"] = actor_id
            self.actor_plans[-1]["plans"] = list()
            for mag_entry in self.trip_plan_from_file[actor_id]:
                if proj:
                    tmp_x, tmp_y = self.proj_coord(
                        mag_entry['orig']['x'], mag_entry['orig']['y'])
                else:
                    tmp_x = mag_entry['orig']['x']
                    tmp_y = mag_entry['orig']['y']
                self.actor_plans[-1]["plans"].append({'actType': "ACTIVITY",
                                                      'x': tmp_x,
                                                      'y': tmp_y,
                                                      'purpose': mag_entry['orig']['purpose'],
                                                      'depart_time_str': '', 'travel_time_str': '',
                                                      'depart_time_sec_dbl': '', 'travel_time_sec_dbl': '',
                                                      'mode': ''})
                self.actor_plans[-1]["plans"].append({'actType': "LEG",
                                                      'x': 0.0, 'y': 0.0, 'purpose': '',
                                                      'depart_time_str': mag_entry['depart_time_str'],
                                                      'travel_time_str': mag_entry['travel_time_str'],
                                                      'depart_time_sec_dbl': mag_entry['depart_time_sec_dbl'],
                                                      'travel_time_sec_dbl': mag_entry['travel_time_sec_dbl'],
                                                      'mode': mag_entry['mode']})
            final_trip = list(self.trip_plan_from_file[actor_id])[-1]
            if proj:
                tmp_x, tmp_y = self.proj_coord(
                    final_trip['orig']['x'], final_trip['orig']['y'])
            else:
                tmp_x = final_trip['orig']['x']
                tmp_y = final_trip['orig']['y']

            self.actor_plans[-1]["plans"].append({'actType': 'ACTIVITY',
                                                  'x': tmp_x,
                                                  'y': tmp_y,
                                                  'purpose': final_trip['dest']['purpose'],
                                                  'depart_time_str': '', 'travel_time_str': '',
                                                  'depart_time_sec_dbl': '', 'travel_time_sec_dbl': '',
                                                  'mode': ''})

    def write_conv_plans(self, filepath, indent):
        with open(filepath, 'w+') as handle:
            json.dump(self.actor_plans, handle, indent=indent)
        print(f'Converted plans written to {filepath}')

    def convert_file(self, input_filepath, output_filepath, indent=None, proj=False):
        self.load_raw_plans(input_filepath)
        self.plan_conversion(proj=proj)
        self.write_conv_plans(output_filepath, indent)


if __name__ == '__main__':
    example = TripPlanToActLegPlan()
    example.set_projection('epsg:2223', 'epsg:4326')
    example.convert_file('Data/samp_actor_plans_apn_coord.json',
                         'Data/MATsim_plan_format.json', indent=2, proj=True)
