import pandas as pd
import json

#TODO
# I need to parameterize the startime, endtime, api_endpoint_suffix, json_filename, parquet_filename

# open file, we use json.load because the file is an object
with open('./data/usgs_api.json') as access_json:
    quakes = json.load(access_json)

# create a variable for the earthquake features
quake_features = quakes['features']

feature_list = []
i = 0
while i < len(quake_features):
    for items in quake_features:
        quake_dic = {}
        quake_dic['type'] = quake_features[i]['type']
        quake_dic['properties_mag'] = quake_features[i]['properties']['mag']
        quake_dic['properties_place'] = quake_features[i]['properties']['place']
        quake_dic['properties_time'] = quake_features[i]['properties']['time']
        quake_dic['properties_updated'] = quake_features[i]['properties']['updated']
        quake_dic['properties_tz'] = quake_features[i]['properties']['tz']
        quake_dic['properties_url'] = quake_features[i]['properties']['url']
        quake_dic['properties_detail'] = quake_features[i]['properties']['detail']
        quake_dic['properties_felt'] = quake_features[i]['properties']['felt']
        quake_dic['properties_cdi'] = quake_features[i]['properties']['cdi']
        quake_dic['properties_mmi'] = quake_features[i]['properties']['mmi']
        quake_dic['properties_alert'] = quake_features[i]['properties']['alert']
        quake_dic['properties_status'] = quake_features[i]['properties']['status']
        quake_dic['properties_tsunami'] = quake_features[i]['properties']['tsunami']
        quake_dic['properties_sig'] = quake_features[i]['properties']['sig']
        quake_dic['properties_net'] = quake_features[i]['properties']['net']
        quake_dic['properties_code'] = quake_features[i]['properties']['code']
        quake_dic['properties_ids'] = quake_features[i]['properties']['ids']
        quake_dic['properties_sources'] = quake_features[i]['properties']['sources']
        quake_dic['properties_types'] = quake_features[i]['properties']['types']
        quake_dic['properties_nst'] = quake_features[i]['properties']['nst']
        quake_dic['properties_dmin'] = quake_features[i]['properties']['dmin']
        quake_dic['properties_rms'] = quake_features[i]['properties']['rms']
        quake_dic['properties_gap'] = quake_features[i]['properties']['gap']
        quake_dic['properties_magType'] = quake_features[i]['properties']['magType']
        quake_dic['properties_type'] = quake_features[i]['properties']['type']
        quake_dic['properties_title'] = quake_features[i]['properties']['title']
        quake_dic['geometry_type'] = quake_features[i]['geometry']['type']
        quake_dic['geometry_long'] = quake_features[i]['geometry']['coordinates'][0]
        quake_dic['geometry_lat'] = quake_features[i]['geometry']['coordinates'][1]
        quake_dic['geometry_focaldepth'] = quake_features[i]['geometry']['coordinates'][2]
        quake_dic['id'] = quake_features[i]['id']
        quake_dic['quake_api_type'] = quakes['type']
        quake_dic['quake_metadata_generated'] = quakes['metadata']['generated']
        quake_dic['quake_metadata_url'] = quakes['metadata']['url']
        quake_dic['quake_metadata_title'] = quakes['metadata']['title']
        quake_dic['quake_metadata_status'] = quakes['metadata']['status']
        quake_dic['quake_metadata_api'] = quakes['metadata']['api']
        quake_dic['quake_metadata_count'] = quakes['metadata']['count']
        feature_list.append(quake_dic)
        i += 1

df = pd.DataFrame.from_records(feature_list, index=None)

df.to_parquet('./data/earthquake2.parquet.gzip', compression='gzip')