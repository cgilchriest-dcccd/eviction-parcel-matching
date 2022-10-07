
import geopandas as gpd
import pandas as pd
import sys
import numpy as np


def geocode_address(address):
    import osmnx as ox
    import geopandas as gpd
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    print(f"Locating: {address}...")
    print(r"°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸")
    #print(f"Pulling {address}...")
    try:
        loc = ox.geocode_to_gdf(address)
        loc = loc.to_crs("EPSG:6584")
        if len(loc) > 1:
            loc = loc.iloc[0:1]
        loc.geometry = loc.centroid
        loc = loc[['display_name','geometry']]
        #print("Done")
        return loc
    except:
        print(r"Error :'(")
        print(fr"Could not find {address} in OSM")
        print("Try a different address nearby")
        raise Error

def transform_to_wkt(gdf,buffer=0,out_crs=4326):
    '''
    Prepare geodataframe to be given to query against the DataBase. 
    Returns WKB of the geometry or a series of wkt geometries. Default crs is 4326.
    
    :param gpd.GeoDataFrame gdf: Geodataframe containing location you want to look at.
    :param int buffer: Distance in miles to buffer around the geometry.
    :param int out_crs: EPSG code for output.
    '''
    import shapely
    import geopandas as gpd
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    if buffer != 0:
        old_crs = gdf.crs.to_epsg()
        gdf = gdf.to_crs("EPSG:6584")
        gdf.geometry = gdf.buffer(buffer * 5280)
        gdf = gdf.to_crs(f"EPSG:{old_crs}")

    #buffer the point/area
    #make 4326 for going back into the sql db
    gdf = gdf.to_crs(f"EPSG:{out_crs}")
    if len(gdf) < 2:        
        wkt = shapely.wkt.dumps(gdf.geometry.iloc[0])
    else:
        wkt = gdf.apply(lambda x: shapely.wkt.dumps(x.geometry),axis=1)
    return wkt

def create_con_cur(db,username,password,address,port):
    import io
    from sqlalchemy import create_engine
    import numpy as np
    print(f"Connecting to {db} as {username}")
    engine = create_engine(f'postgresql://{username}:{password}@{address}:{port}/{db}')
    conn = engine.raw_connection()
    cur = conn.cursor()
    print("Done")
    return conn,cur

def pull_by_name_tx_only(name,token):
    import pandas as pd
    import sqlite3 
    import geopandas as gpd
    import matplotlib.pyplot as plt 
    import folium
    import requests
    import json

    toke = f"&api_token={token}"
    name = name.replace(" ","%20")
    url = "https://api.opencorporates.com/v0.4.8/companies/search?q=" + name + toke
    
    full_names = []
    resp = requests.get(url)
    if resp.status_code == 200:
        result = json.loads(resp.content)
        names = [x['company']['name'] for x in result['results']['companies'] if (x['company']['jurisdiction_code'] == 'us_tx') & (x['company']['current_status'] == 'In Existence')]
        full_names.append(names)
    total_pages =  result['results']['total_pages']
    count = 2
    while count <= total_pages:
        pull_url = url + r"&page=%d" % count
        resp = requests.get(pull_url)
        if resp.status_code == 200:
            result = json.loads(resp.content)
            names = [x['company']['name'] for x in result['results']['companies'] if (x['company']['jurisdiction_code'] == 'us_tx') & (x['company']['current_status'] == 'In Existence')]
            full_names.append(names)
            count += 1 
    name_list = sum(full_names, [])
    return name_list

def pull_by_address(address,token):
    import pandas as pd
    import sqlite3 
    import geopandas as gpd
    import matplotlib.pyplot as plt 
    import folium
    toke = f"&api_token={token}"
    addy = address.replace(" ","%20")
    url = "https://api.opencorporates.com/v0.4/companies/search?registered_address=" + addy
    import requests
    import json
    full_names = []
    resp = requests.get(url + toke)
    if resp.status_code == 200:
        result = json.loads(resp.content)
        names = [x['company']['name'] for x in result['results']['companies'] if (x['company']['jurisdiction_code'] == 'us_tx') & (x['company']['current_status'] == 'In Existence')]
        full_names.append(names)
    total_pages =  result['results']['total_pages']
    count = 2
    while count <= total_pages:
        pull_url = url + r"&page=%d" % count + toke
        resp = requests.get(pull_url)
        if resp.status_code == 200:
            result = json.loads(resp.content)
            names = [x['company']['name'] for x in result['results']['companies'] if (x['company']['jurisdiction_code'] == 'us_tx') & (x['company']['current_status'] == 'In Existence')]
            full_names.append(names)
        count += 1 
    name_list = sum(full_names, [])
    return name_list

#is there a link between apartment renovation activity and evictions?
#are there certain owners who, when they purchase a property, are more likely to escalate evictions? 

#look at properties that improved quality yOy
#look at properties that had a remodel 
#chart this specific one out over time and show what she did and how it worked and use pictures too


#pull eviction records
def pull_from_github(token,owner,repo):

    import json
    import requests 
    import pandas as pd
    import geopandas as gpd
    from io import StringIO

    print("Getting SHA...")
    #get sha
    rsha = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents",
        headers={
            'accept': 'application/vnd.github.v3.raw',
            'authorization': f'token {token}'
                }
        )

    filesha = [x for x in json.loads(rsha.text) if x['name'] == "DallasCounty_EvictionRecords.csv"][0]['sha']
    
    #get file from data api using sha
    print("Getting file..")
    r = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/git/blobs/{filesha}",
        headers={
            'accept': 'application/vnd.github.v3.raw',
            'authorization': f'token {token}'
                }
        )
    
    print("Processing...")
    string_io_obj = StringIO(r.text)
    evictionsdf = pd.read_csv(string_io_obj, low_memory=False)

    #make gdf
    evictionsdf = evictionsdf.dropna(subset=['lat','lon'])
    evictionsgdf = gpd.GeoDataFrame(evictionsdf,geometry=gpd.points_from_xy(evictionsdf['lon'],evictionsdf['lat']),crs="EPSG:4326")
    evictionsgdf = evictionsgdf.to_crs("EPSG:2276")
    print("Done!")
    return evictionsgdf

def pair_parcels_to_permits(parcels,permits):
    #Create a linking of permits with parcels data
    #parcels need to have account_info table attached
    print("Analyzing...")
    #turn off warnings
    pd.options.mode.chained_assignment = None
    import warnings
    warnings.filterwarnings('ignore')
    
    #Prepare the parcel frame
    try:
        parcels[f'property_address'] = parcels['street_num'].astype(str) + " " + parcels['full_street_name'] 
        parcels[f'billing_address'] = parcels['owner_address_line2'].fillna("NONE")
        parcels = parcels.drop_duplicates("account_num").query("division_cd != 'BPP'")
    except:
        print("Error- need to join an 'account_info' table to parcels")
    
    #step 1 - try to join the parcels to the permuits
    import re
    import rapidfuzz as rf
    permits['street_num'] = permits.apply(lambda x: re.sub('\D',"", x['in_singleline'].split(" ")[0]), axis=1).astype(float)
    permits['street_name'] = permits.apply(lambda x: re.sub('\d+',"", x['in_singleline'].split(",")[0]), axis=1).str.strip()
    
    d = permits.merge(parcels,on='street_num')
    d['street_name_ratio'] = d.apply(lambda x: rf.fuzz.QRatio(x['street_name'],x['full_street_name']),axis=1)

    #sort by most likely match
    d = d.sort_values(by=['in_singleline','street_name_ratio'],ascending=[True,False])
    d['match_method'] = 'street_name'
    #hold this for a second

    #getting nearest 5 parcels
    import numpy as np 
    from scipy.spatial import cKDTree
    from shapely.geometry import Point

    #make arrays 
    #issues with using centroids - reducing accuracy - will need to deal with in v2
    nA = np.array(list(permits.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(parcels.centroid.geometry.apply(lambda x: (x.x, x.y))))
    #make btree
    btree = cKDTree(nB)
    
    #get distance and nearest
    dist, idx = btree.query(nA, k=5)
    
    #make hold frame
    matches = pd.DataFrame()
    
    #loop through each permit and look at nearest parcels
    for i,z in enumerate(idx):
        #parcel slice
        par_sl = parcels.iloc[z].drop(columns="geometry").reset_index(drop=True)[['account_num','property_address','street_num']]
        par_sl['near_rank'] = [x +1 for x in par_sl.index.tolist()]
        
        #permit slice
        perm_sl = permits.iloc[i:i+1]
    
        match = pd.concat([perm_sl]*5).reset_index(drop=True).merge(par_sl.iloc[:5],left_index=True,right_index=True,suffixes=('_permit_s','_parcel_s'))
        matches = pd.concat([matches,match])
    
    #clean it up - final matching
    print("Scoring...")
    finalperm = permits[['in_singleline','street_num','street_name']].merge(
        d[['in_singleline','property_address','street_num','street_name_ratio','match_method','account_num']],on='in_singleline',how='left',suffixes=('','_parcel')).merge(
            matches[['in_singleline','street_num_permit_s','street_num_parcel_s','near_rank','account_num']],on=['in_singleline','account_num'],how='outer',suffixes=('_permit','_spatial_match'))
    
    tomatch = finalperm['in_singleline'].unique().tolist()
    print(f"Unique permit addresses to score: {len(tomatch)}")

    #confirmed ones 
    sm = finalperm.query("match_method == 'street_name'")
    conf1 = sm.query("street_name_ratio > 85")
    matchl1 = [(x,y,z) for x,y,z, in zip(conf1['in_singleline'].tolist(),conf1['account_num'].tolist(),['NAME_MATCH'] * len(conf1))]

    conf2 = finalperm[~(finalperm['in_singleline'].isin(conf1['in_singleline'].unique()))] 
    matchl2 = []
    for x in conf2['in_singleline'].unique():
        t = conf2.query("in_singleline == @x")
        t_per = t.dropna(subset=['near_rank'])
        t_par = t.dropna(subset=['match_method'])
        if any(x in t_per['account_num'] for x in t_par['account_num']):
            act = [y for y in t_par if x in [t_per]][0]
            matchl2.append([x,act,'SPATIAL_AND_NAME_MATCH'])
        else:
            act = t_per['account_num'].iloc[0]
            matchl2.append([x,act,'SPATIAL_ONLY_MATCH'])

    final_matches = pd.DataFrame(matchl1+matchl2,columns=['in_singleline',
        'account_num',
        'match_type'])
    print("Done!")
    return final_matches
    
def pair_parcels_to_evictions(parcels,evictions):
    #ignore warnings
    print("running...")
    import rapidfuzz as rf
    pd.options.mode.chained_assignment = None
    
    #get the parcels property address
    parcels[f'property_address'] = parcels['street_num'].astype(str) + " " + parcels['full_street_name'] 
    parcels[f'billing_address'] = parcels['owner_address_line2'].fillna("NONE")
    parcels = parcels.query("division_cd != 'BPP'")
    
    evictions['street_num'] = evictions.apply(lambda x: re.sub('\D',"", x['df_address'].split(" ")[0]), axis=1).astype(float)
    d = evictions.merge(parcels,on='street_num')
    d['street_name'] = d.apply(lambda x: re.sub('\d+',"", x['df_address']), axis=1).str.strip()
    d['ratio'] = d.apply(lambda x: rf.fuzz.ratio(x['street_name'],x['full_street_name']),axis=1)

    import numpy as np 
    from scipy.spatial import cKDTree
    from shapely.geometry import Point
    import re
    evictions = evictions.to_crs("EPSG:4326")
    parcels = parcels.to_crs("EPSG:4326")
    #make arrays    
    nA = np.array(list(evictions.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(parcels.centroid.geometry.apply(lambda x: (x.x, x.y))))
    #make btree
    btree = cKDTree(nB)
    
    #get distance and nearest
    dist, idx = btree.query(nA, k=10)
    
    #make hold frame
    matches = pd.DataFrame()
    
    #loop through each permit and look at nearest parcels
    for i,z in enumerate(idx):
        #parcel slice
        par_sl = parcels.iloc[z].drop(columns="geometry").reset_index(drop=True)[['account_num','property_address','street_num']]
        par_sl['near_rank'] = [x +1 for x in par_sl.index.tolist()]
        
        #permit slice
        evc_sl = evictions.iloc[i:i+1]
        

        evc_sl['street_num'] = evc_sl.apply(lambda x: re.sub('\D',"", x['df_address'].split(" ")[0]), axis=1).astype(float)
        
        #if the street num matches, join on that one
        if evc_sl['street_num'].item() in par_sl['street_num'].tolist():
            match = evc_sl.merge(par_sl,on='street_num').iloc[0:1]
            match['addr_match'] = 'Y'
        
        #otherwirse join the 3 nearest matches
        else:
            match = pd.concat([evc_sl]*3).reset_index(drop=True).merge(par_sl.iloc[:3],left_index=True,right_index=True,suffixes=('','_par'))
            match['addr_match'] = 'N'
        matches = pd.concat([matches,match])
    return matches
