from MagDataToPlansByPidAndMaz import MagDataToPlansByPidAndMaz
from AgentPlansToJson import AgentPlansToJson
from TripPlanToActLegPlan import TripPlanToActLegPlan

mag_writer = MagDataToPlansByPidAndMaz()
mag_writer.read_mag_csv('raw/output_disaggTripList.csv')

agent_plans = AgentPlansToJson()
agent_plans.maz_plan_dict = mag_writer.actor_dict
agent_plans.assign_apn_to_agents('data/full_maricop_parcel_coord_by_MAZ.json')
agent_plans.to_dict()

trip_creation = TripPlanToActLegPlan()
trip_creation.trip_plan_from_file = agent_plans.apn_plan_dict
trip_creation.plan_conversion(proj=False)
trip_creation.write_conv_plans(
    filepath='data/full_MATsimPlans_2223.json', indent=None)
