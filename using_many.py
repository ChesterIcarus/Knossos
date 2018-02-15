from plan_to_matplan import PlanToMatplan as ptm
from apn_linking import apn_linking as al


files = {'parcel': '../Shapefiles/Cleaned/test_dirty_point.geojson', 'maz':'real_maz/min_maz.geojson'} 
osm = '../Shapefiles/Cleaned/cleaned_matsim.xml'
db_param = {'user':'root', 'database':'cleaned.db', 'table_name':'clean', 'drop':True, 'host':'localhost'}
x = al()
x.load_maz_and_parcel(files)
x.get_valid_ways(osm)
x.apn_bounding()
# x.db_connection(db_param)
x.assign_apn_to_MAZ(False)

y = ptm()
y.connect_apn_db(_file="cleaned.db")
y.connect_plan_db(_file="actor_plan.db")
# y.bounded_maz_creation('maz_dict.json', False, input_maz=mazs)
# y.bounded_maz_creation(None, 'test.txt', maz_in_memory=True, overwrite=True)
y.bounded_maz_creation(True, 'test.txt', maz_in_memory=True, overwrite=True, linked_dict=x.osm_apn_maz)

# y.load_plans_from_json("plans_by_maz_and_pid.json")
y.load_plans_from_sqlite('trips')
y.maz_to_plan_db("clean", "maz", "actor_plan_testing.txt")