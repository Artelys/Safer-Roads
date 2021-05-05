#-*- coding:utf8 -*-
# Import libraries
from utils import mapping_geom 
import pandas as pd
import numpy as np
from geopy import Nominatim, BANFrance
from geopy.exc import GeocoderTimedOut
import math
import warnings
warnings.filterwarnings('ignore')
from get_connection import get_connection
from elasticsearch import helpers
import requests
import time
import json
import os
import geopandas as gpd
from shapely.geometry import MultiLineString, Point, mapping, Polygon
import geog
from pyproj import Proj, transform
import csv
from scipy.linalg import norm
import utm
from haversine import haversine


#------------------------------------------------------------------------------#
#---------------------------Elastic Search ------------------------------------#
#------------------------------------------------------------------------------#

def _activ_elasticsearch():
    '''
    Activate Elastic Search
    '''

    # Elastic Search parameters
    return get_connection()


def query_accident(es, index, columns):
    ''' Get the data stored in the index in ES.
    Index : Coporels, materiels
    Columns : will be the useful columns to locate an accident.
    Return a dataframe with those columns.
    '''

    #--- Request
    query = {"_source": {"includes": columns},
             "size": 10000,}
    res = es.search(index=index, body=query)

    #--- Put the request in a dataframe
    df = pd.DataFrame(columns= columns)
    for r in res["hits"]["hits"]:
        df = df.append(r['_source'], ignore_index=True)

    return df

def post_link_id(es, index, gdf, file):
    ''' Post link_id on Elastic Search
    '''
    json_file = str(str(file) + ".geojson")
    # gdf.Distance_Point_LINK_ID.to_csv(str(index)+'.csv', sep=';')
    if index == 'baac_link_id':
        df = pd.DataFrame(gdf.ID_BAAC.values.reshape(-1,1), columns=['ID_BAAC'])
        df['ID_COMMUNE_ACCIDENT'] = gdf['ID_COMMUNE_ACCIDENT']
        df['ID_INTERSECTION'] = gdf['ID_INTERSECTION']
        df['ADRESSE'] = gdf['ADRESSE']
        df['latitude'] = gdf['latitude']
        df['longitude'] = gdf['longitude']
        df['LINK_acc'] = gdf['LINK_acc']
        df['Distance_Point_LINK_ID'] = gdf['Distance_Point_LINK_ID']
        df.to_csv(str(index)+'.csv', sep=';')
    elif index == 'ass_link_id':
        df = pd.DataFrame(gdf['ADRESSE'].values.reshape(-1,1), columns=['ADRESSE'])
        df['ville'] = gdf['ville']
        df['INSEE'] = gdf['INSEE']
        df['latitude'] = gdf['latitude']
        df['longitude'] = gdf['longitude']
        df['LINK_acc'] = gdf['LINK_acc']
        df['Distance_Point_LINK_ID'] = gdf['Distance_Point_LINK_ID']
        df.to_csv(str(index)+'.csv', sep=';')
    gdf.to_file(json_file, driver='GeoJSON')
    mapping_geom(es, index, json_file)


#------------------------------------------------------------------------------#
#---------------------------GEOLOCATE  ----------------------------------------#
#------------------------------------------------------------------------------#

def _geocoding(Location, geolocator):
    '''
    Geocode an adress. If there is a time error, it is relaunched.
    '''
    try:
        return geolocator.geocode(Location)
    except GeocoderTimedOut:
        return geocoding(Location)

def _get_lat_long(df, geolocator, baac=True):
    '''
    Get the latitude and longitude of the kilometric point of roads.
    Those are found with the address and a geocoder.

    TODO : FONCTION QUI PEUT ETRE EVITEE SI ON A LES LAT LONG DE JEROME
    '''
    if baac:
        # print(df)
        df['GPS_LATITUDE'][df['GPS_LATITUDE'] == ''] = np.nan
        df['GPS_LONGITUDE'][df['GPS_LONGITUDE'] == ''] = np.nan
        df = df.assign(latitude = df['GPS_LATITUDE'].astype(float))
        df = df.assign(longitude = df['GPS_LONGITUDE'].astype(float))
    else:
        df = df.assign(latitude = np.nan)
        df = df.assign(longitude = np.nan)

    for i in range(len(df)):
        adr = df.ADRESSE.iloc[i]

        if  isinstance(adr, str):
            adr_list = list(adr)

            # Column BAAC
            if baac:
                adresse = adr + ', ' + str(df['ID_COMMUNE_ACCIDENT'].iloc[i]) \
                          + ', Loiret, France'
            # Column Assurance
            else:
                adresse = adr +', '+ str(df['ville'].iloc[i]) + ', ' \
                + str(df['INSEE'].iloc[i])+ ', Loiret, France'

            # If the adress begin by a number. It is considered as usable.
            # TODO : CONDITION A ENELEVER SI ON A UN BON GEOCODER
            if len(adr_list)>0 and adr_list[0].isdigit():
                try :
                    # NOUVEAU GEOCODER A METTRE ICI
                    location = _geocoding(adresse, geolocator)
                except (GeocoderTimedOut, AttributeError):
                    pass

                if location != None:
                    df['latitude'].iloc[i] = location.latitude
                    df['longitude'].iloc[i] = location.longitude


    return df

def _get_lat_long_pk(es, df_pk, df_bornes, df_nat_loiret):
    '''
    Get the latitude and longitude of the kilometric point of roads.
    Those are found in the Opendata/bornages.
    '''
    df_pk = df_pk.assign(LINK_ID = np.nan)
    indices = df_pk.index[df_pk.PK_BORNE.notna()]
    for i in (indices.unique()):
        voie = df_pk.VOIE.loc[i]
        pk = df_pk.PK_BORNE.loc[i]
        dist_pk = df_pk.PK_METRE.loc[i]
        type_voie = df_pk.ID_CATEGORIE.loc[i]
        link_acc = 0

        lat_long = False

        if df_pk.GPS_LONGITUDE.iloc[i] != '' and not math.isnan(float(df_pk.GPS_LONGITUDE.iloc[i])):
            lat_long = True
            point_lat_long = [float(df_pk.GPS_LONGITUDE.iloc[i]),
                              float(df_pk.GPS_LATITUDE.iloc[i])]
            res = _get_link_in_envelope(es, point)
            if len(res)>0 and res['hits']['total']['value'] >0:
                link_id_lat_long, _ = _get_closest_link(point, res)
                link_acc = link_id_lat_long

        # print(voie, PK)
        elif voie != '' and not math.isnan(pk):
            pk1 = pk + 1
            # if not math.isnan(voie) and not math.isnan(PK):
            if isinstance(voie, str):
                voie = voie.lower()
                if 'rd' in voie:
                    voie = voie.replace('rd', '')
                if 'd' in voie:
                    voie = voie.replace('d', '')
                if 'a' in voie:
                    voie = voie.replace('a', '')
                if ' ' in voie:
                    voie = voie.replace(' ', '')
            # if not isinstance((voie, PK), str) adn not math.isnan((voie, PK)):

            #--- Bornes département + nationales
            if type_voie in ['20103', 20103, '20110', 20110, '20102',20102,
                             '20109', 20109]:
                #départementale 20103', 20103, '20110', 20110
                if type_voie in ['20102',20102, '20109', 20109]:
                #nationale '20102',20102, '20109', 20109
                    if '20' not in str(voie):
                        voie = '20'+str(voie)

                filter_PK = df_bornes[(df_bornes.RD == voie) &
                                      (df_bornes.PR == pk) &
                                      (df_bornes.ABS == 0)]
                if len(filter_PK) > 0:
                    # point_pk = filter_PK.geometry_4326.values[0]
                    # df_pk.latitude_PK.iloc[i] = point_pk.y
                    # df_pk.longitude_PK.iloc[i] = point_pk.x
                    point = [filter_PK.geometry_4326.values[0].x,
                             filter_PK.geometry_4326.values[0].y]
                    others_properties = ['properties.Shape_Length']
                    res = _get_link_in_envelope(es, point,
                        others_properties=others_properties)
                    if len(res)>0 and res['hits']['total']['value'] >0:
                        link_id, coord, prop = _get_closest_link( point, res,
                                                                ['Shape_Length',])
                    else:
                        link_acc = -1000

                filter_PK1 = df_bornes[(df_bornes.RD == voie) &
                                       (df_bornes.PR == pk1) &
                                       (df_bornes.ABS == 0)]
                if len(filter_PK1) >0 :
                    point1 = [filter_PK1.geometry_4326.values[0].x,
                              filter_PK1.geometry_4326.values[0].y]
                    res1 = _get_link_in_envelope(es, point1,
                                                others_properties=others_properties)
                    if len(res1)>0 and res1['hits']['total']['value'] >0:
                        link_id1, coord1, prop1 = _get_closest_link( point1, res1,
                                                                ['Shape_Length',])
                    else:
                        link_acc = -1000
                else:
                    link_id1 =-1000
                    prop1 = [-1000]

            #--- Bornes autoroute
            elif type_voie in ['20101', 20101, '20112', 20112]:
                #autoroute
                filter_PK = df_nat_loiret[(df_nat_loiret.route_number == voie) &
                                          (df_nat_loiret.pr == pk)]
                if len(filter_PK) > 0:
                    # df_pk.latitude_PK.iloc[i] = filter_PK.latitude_PK_nat.values[0]
                    # df_pk.longitude_PK.iloc[i]= filter_PK.longitude_PK_nat.values[0]
                    point = [filter_PK.longitude_PK_nat.values[0],
                             filter_PK.latitude_PK_nat.values[0]]
                    others_properties = ['properties.Shape_Length',]
                    res = _get_link_in_envelope(es, point,
                                            others_properties=others_properties)
                    if len(res)>0 and res['hits']['total']['value'] >0:
                        link_id, coord, prop = _get_closest_link( point, res,
                                                              ['Shape_Length',])
                    else:
                        link_acc = -1000

                filter_PK1 =  df_nat_loiret[(df_nat_loiret.route_number == voie) &
                                            (df_nat_loiret.pr == pk1)]
                if len(filter_PK1) >0:
                    point1 = [filter_PK1.longitude_PK_nat.values[0],
                              filter_PK1.latitude_PK_nat.values[0]]
                    others_properties = ['properties.Shape_Length',]
                    res1 = _get_link_in_envelope(es, point1,
                                            others_properties=others_properties)
                    if len(res1)>0 and res['hits']['total']['value'] >0:
                        link_id1, coord, prop = _get_closest_link( point, res,
                                                              ['Shape_Length',])
                    else:
                        link_acc = -1000

            #--- Règles pour attribuer le link_ID
            if link_acc != -1000 and not lat_long:
                if link_id == link_id1:
                    link_acc = link_id
                elif dist_pk >= 750:
                    link_acc = link_id1
                elif dist_pk <= 250:
                    link_acc = link_id
                elif dist_pk < prop[0]:
                    link_acc = link_id
                elif (1000 - dist_pk) < prop1[0]:
                    link_acc = link_id1
                elif dist_pk <= 500:
                    link_acc = link_id
                elif dist_pk > 500:
                    link_acc = link_id1
                else:
                    link_acc = -1000

        df_pk.LINK_ID.loc[i] = link_acc
    return df_pk

def _get_link_in_envelope(es, point, others_properties=None, index_name="voirieref"):
    '''
    Get the nearest link to a point (accident address).
        We look at a box around the accident and get all the links inside the
        box.
    Return the request results.
    '''

    properties = ["geometry", "LINK_ID"]
    if others_properties is not None:
        for prop in others_properties:
            properties += [prop]
    polygon = mapping(Polygon(geog.propagate(Point(point), np.linspace(0, 360, 50), 500)))["coordinates"]
    query = {"_source": { "includes": properties},
                          "size": 10000,
                                       "query":{"bool": {
                                                    "filter": {"geo_shape": {"geometry": {"shape": {
                                                                                            "type": "polygon",
                                                                                            "coordinates":polygon,
                                                                                            "relation": "within"}}}},
                                                    "must_not":[
                                                        {"exists":{"field":"point"}}
                                                        ]}}}
    res = es.search(index=index_name, body=query)
    return res


def _get_closest_link(point, res, others_properties=None):
    # '''
    # Get the closest link from the accident coordinates.
    #     Point: The coordinates of the accident. [lat, long]
    #     Res: Request result (res = es.search(index="voirieref", body=query))
    # Return nearest link ID from accident.
    # '''
    # link_id_list = [r['_source'] for r in res['hits']['hits']]

    # #--- compute distance line/point
    # dist_line = []
    # for r in res["hits"]["hits"]:
    #     pts = r['_source']['geometry']['coordinates'][0]
    #     dist_pt = []
    #     for i in range(len(pts)-1):
    #         #DISTANCE A CHANGER ET METTRE COORD X,Y
    #         vec_a = np.array(pts[i+1]) - np.array(pts[i])
    #         vec_b = np.array(pts[i]) - np.array(point)
    #         a = vec_a @ vec_b / norm(vec_a)
    #         d = np.sqrt(norm(vec_b) - a**2)
    #         dist_pt += [d]
    #     dist_line += [dist_pt]

    # #--- Find the minimal distance
    # d_min=100
    # for i in range(len(dist_line)):
    #     for d in dist_line[i]:
    #         if d < d_min :
    #             d_min =d
    #             index_min = i
    # nearest_link = link_id_list[index_min]['properties']['LINK_ID']

    # if others_properties is not None:
    #     prop_result = []
    #     for prop in others_properties:
    #         prop_result += [link_id_list[index_min]['properties'][prop]]
    #         coordinates = link_id_list[index_min]['geometry']
    #     return nearest_link, coordinates, prop_result

    # return nearest_link, d_min
    link_id_list = [r['_source'] for r in res['hits']['hits']]
    dist_line = []
    for r in res["hits"]["hits"]:
        pts = r['_source']['geometry']['coordinates'][0]
        dist_pt = []
        for i in range(len(pts)-1):
            """
            R = 6371

            bearingAC = np.arctan2(np.sin(pts[i][0] - point[0]) * np.cos(pts[i][1]), np.cos(point[1]) * np.sin(pts[i][1]) - np.sin(point[1]) * np.cos(pts[i][1]) * np.cos(pts[i][0] - point[0]))
            bearingAB = np.arctan2(np.sin(pts[i+1][0] - point[0]) * np.cos(pts[i+1][1]), np.cos(point[1]) * np.sin(pts[i+1][1]) - np.sin(point[1]) * np.cos(pts[i+1][1]) * np.cos(pts[i+1][0] - point[0]))

            distanceAC = np.arccos(np.sin(point[1]) * np.sin(pts[i][1]) + np.cos(point[1]) * np.cos(pts[i][1]) * np.cos(pts[i][0] - point[0])) * R

            distance = np.arcsin(np.sin(distanceAC/R) * np.sin(bearingAC - bearingAB)) * R
            dist_pt += [distance]
            """
            a = haversine((pts[i][1], pts[i][0]), (point[1], point[0]), unit='m')
            b = haversine((pts[i][1], pts[i][0]),(pts[i+1][1], pts[i+1][0]), unit='m')
            c = haversine((pts[i+1][1], pts[i+1][0]), (point[1], point[0]), unit='m')
            p1 = (0, 0)
            p2 = (b, 0)
            x = - (c**2-b**2-a**2)/(2*b)
            y = np.sqrt(a**2-x**2)
            p3 = (x, y)
            p = (a + b + c)/2 #perimetre
            A = np.sqrt(p*(p-a)*(p-b)*(p-c)) #aire
            H = 2*A/b #hauteur

            cosalpha = np.sqrt(a**2 - H**2)/a
            if cosalpha >= 0:
                if x < 0 or x > b:
                    d = min(a, c)
                else:
                    d = H
                dist_pt += [d]
        dist_line += [dist_pt]
    d_min=1000000000
    index_min = 1000000000
    for i in range(len(dist_line)):
        for d in dist_line[i]:
            if d < d_min :
                d_min =d
                index_min = i
    if index_min == 1000000000:
        nearest_link = np.nan
        id = 0
        print('nearest_link not found')
    else :
            nearest_link = link_id_list[index_min]['LINK_ID']
            id = res["hits"]["hits"][index_min]["_id"]
    if others_properties is not None:
        prop_result = []
        for prop in others_properties:
            prop_result += [link_id_list[index_min][prop]]
            coordinates = link_id_list[index_min]['geometry']
        return nearest_link, coordinates, prop_result
    return nearest_link, d_min, id



def _get_link_id(es, df, not_found_ft=False, baac=False):
    ''' Get the link for all the addresses.
    '''
    df = df.assign(LINK_acc = np.nan)
    df = df.assign(Distance_Point_LINK_ID = np.nan)
    df_lat_long = df[df.latitude.notna()]

    for i in df_lat_long.index:
        point = [df_lat_long.longitude.loc[i], df_lat_long.latitude.loc[i]]

        res = _get_link_in_envelope(es, point)
        if len(res)>0 and res['hits']['total']['value'] >0:
            link_id, distance = _get_closest_link(point, res)
            df.LINK_acc.loc[i] = link_id
            df.Distance_Point_LINK_ID.loc[i] = distance
    return df




def get_location_accident(baac=False, ass=False, PK=False):
    '''
    Locate accident:
    - Geocode accident address,
    - Find closest link to accident geolocation
    - Associate a link to each accident

    Location for BAAC and Assurance.
    Return the link_id associate to each accident (if found).
    '''

    #--- Launch Elastic Search
    es = _activ_elasticsearch()
    geolocator2 = BANFrance(timeout=30)

    #--------------------------------------------------------------------------#
    #--------------------------------- BAAC -----------------------------------#
    #--------------------------------------------------------------------------#
    if baac:
        start_time = time.time()
        #--- Get data
        df_baac = query_accident(es, "baac_acc", columns=["ID_BAAC", "ADRESSE",
                                "ID_COMMUNE_ACCIDENT", "ID_INTERSECTION",
                                "GPS_LATITUDE", "GPS_LONGITUDE"])
        #--- Locate
        # geolocator1 = Nominatim(timeout=30)
        geolocator2 = BANFrance(timeout=30)
        df_baac = _get_lat_long(df_baac, geolocator2, baac=True)

        #--- Locate on link_id
        df_baac = _get_link_id(es, df_baac, not_found_ft=False, baac=True)
        gdf_baac = gpd.GeoDataFrame(df_baac,
            geometry=gpd.points_from_xy(df_baac.longitude, df_baac.latitude))

        #--- Post the link id on ES
        post_link_id(es, 'baac_link_id', gdf_baac, 'gdf_baac')

    if PK:
        #PASSER BORNAGES DEPARTEMENT + AUTOROUTE EN INDEX ELASTIC SEARCH
        dataPath = "D:/BE GOOD Safer Roads Loiret - échanges internes/BE GOOD Safer Roads Loiret - échanges internes/1 - Data/"
        #--- Get PK dep
        dataPathBaac = dataPath + "BAAC/baac_45.csv"
        df_baac = pd.read_csv(dataPathBaac)
        dataPathBaacPK = dataPath + "BAAC/baac_pk.csv"
        df_pk = pd.read_csv(dataPathBaacPK)
        df_pk = df_pk.drop_duplicates()
        df_baac_pk = pd.merge(df_baac, df_pk, on='ID_BAAC', how='outer')
        # df_baac_pk = df_baasc_pk.loc[:100]

        #--- Get PK département
        bornesPath = dataPath + 'OpendataCD45/bornage-bornes/BORNAGE_BORNES.shp'
        df_bornes = gpd.read_file(bornesPath)
        #only keep numerical road
        df_bornes['dep_num'] = pd.to_numeric(df_bornes['RD'], errors='coerce')
        #change coordiate
        df_bornes['geometry_4326'] = df_bornes['geometry'].to_crs(epsg=4326)
        #--- Get PK nationaux
        bornesNatPath = dataPath + 'bornes-2019.csv'
        df_nat = pd.read_csv(bornesNatPath, sep=';')
        df_nat_loiret = df_nat[df_nat.depPr == 45]
        df_nat_loiret['route_type'] = df_nat_loiret['route'].astype(str).str[0]
        df_nat_loiret['route_number'] = df_nat_loiret['route'].astype(str).str[-2]  \
                                 + df_nat_loiret['route'].astype(str).str[-1]
        #change coordinate
        inProj = Proj(init='epsg:2154')
        outProj = Proj(init='epsg:4326')
        x1,y1 = df_nat_loiret.x.values, df_nat_loiret.z.values
        x2,y2 = transform(inProj,outProj,x1,y1)
        df_nat_loiret = df_nat_loiret.assign(latitude_PK_nat = y2)
        df_nat_loiret = df_nat_loiret.assign(longitude_PK_nat = x2)

        #--- Coordinates
        df_baac_pk = _get_lat_long_pk(es, df_baac_pk, df_bornes, df_nat_loiret)

        df_baac_pk.to_csv('PK.csv', sep=';')


    #--------------------------------------------------------------------------#
    #------------------------------- Assurance --------------------------------#
    #--------------------------------------------------------------------------#
    if ass:
        #--- Get data
        # dataPathAss = dataPath + "Mondial-Thelem/Data Thélem-Mondial - Anonymisée - Août 2019.xlsx"

        #ATTENTION: ICI LES COLONNES DES ACCIDENTS MATERIELS ONT ETE CHANGEES
        df_ass = query_accident(es, "assurance_acc",
                                    columns=['ville', 'ADRESSE', 'INSEE'])
        # print('len after query', len(df_ass))
        # print(n)
        #                         columns=["(Eve) Ville du lieu d'évènement",
        #                                  "(Eve) Adresse du lieu d'évènemen",
        #                                  "(Eve) Code Insee du lieu d'évène"])
        # print(df_ass)

        # df_ass = pd.read_excel(dataPathAss, sheet_name='FINAL')

        #Keep Loiret only
        # df_ass =df_ass[df_ass["INSEE"].notna()]
        # df_loiret = df_ass[df_ass['INSEE'].str.startswith('45')]

        # #Rename columns
        # df_loiret.rename(columns={"(Eve) Ville du lieu d'évènement":'ville'}, inplace=True)
        # df_loiret.rename(columns={"(Eve) Adresse du lieu d'évènemen":'ADRESSE'}, inplace=True)
        # df_loiret.rename(columns={"(Eve) Code Insee du lieu d'évène":'INSEE'}, inplace=True)
        df_loiret = df_ass.copy()
        #--- Coordinates
        df_loiret = _get_lat_long(df_loiret, geolocator2, baac=False)

        #--- Locate on link_id
        df_loiret = _get_link_id(es, df_loiret, not_found_ft=False)
        gdf_ass = gpd.GeoDataFrame(df_loiret,
            geometry=gpd.points_from_xy(df_loiret.longitude, df_loiret.latitude))

        #--- Post the link id on ES
        post_link_id(es, 'ass_link_id', gdf_ass, 'gdf_ass')

#import time
#start_time = time.time()
#get_location_accident(baac=True, ass=True, PK=True)
#end_time = time.time()
#time_diff = end_time - start_time
#print('Time needed',time_diff )




#GENERAL :
#ATTENTION LES DONNEES QUE L'ON RECUPERE DE ELASTICSEARCH SONT SOUVENT DES STR
#MEME SI CE SONT DES NOMBRES




# ts better to convert to utm coordinates, and treat that as x and y.

# import utm
# u = utm.from_latlon(12.917091, 77.573586)
# The result will be (779260.623156606, 1429369.8665238516, 43, 'P')
# The first two can be treated as x,y coordinates, the 43P is the UTM Zone, which can be ignored for small areas (width upto 668 km).
