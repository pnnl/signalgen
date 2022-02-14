import shutil
import copy
import os
import json
import schema_grapher
import uuid
import random
import math
import string
import time
import rdflib
import gzip
import numpy as np
import geopy.distance
import datetime
import hashlib
import logging
from multiprocessing import Pool
from faker import Faker

baseURI = 'http://schema.localhost/'

def SigDig(num):
    """Function that rounds a number  to it's most significt digit"""
    return round(num, -int(math.floor(math.log10(abs(num)))))

def RanString():
    """Function to generate a random string of 32 characters"""
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)])

def DictIndex(varDict, var, value):
    """Finds the index of a value in a dictionary"""
    return next((index for (index, d) in enumerate(varDict) if d[var] == value), None)

def AttrIndex(varArr, value):
    """Returns the index of a list of lists where the first element of the inner list is the value"""
    return next((i for i in range(len(varArr)) if varArr[i][0] == value), None)

def RandLatLon(y0,x0,distance):
    """Returns a random latitude and longitude some distance away from a given latitude and longitude"""
    r = distance/ 111300
    u = np.random.uniform(0,1)
    v = np.random.uniform(0,1)
    w = r * np.sqrt(u)
    t = 2 * np.pi * v
    x = w * np.cos(t)
    x1 = x / np.cos(y0)
    y = w * np.sin(t)
    return [y0 +y, x0+x1]

def GenerateDynGeo(edges,locs,tbg):
    """An algorithm that places geolocations at locations that satisfy the constraints of the graph"""
    locsgen = [i[0] for i in locs]
    subedges = [i for i in edges if (i[0] == tbg or i[1] == tbg) and (i[0] in locsgen or i[1] in locsgen)]
    loc = locs[locsgen.index(subedges[0][0])] if subedges[0][0] != tbg else locs[locsgen.index(subedges[0][1])]
    condis = subedges[0][2]
    nloc = None
    sat = False
    while(not(sat)):
        sat = True
        nloc = RandLatLon(loc[1][0], loc[1][1], condis)
        for i in subedges:
            if i[0] == tbg:
                sat = True if sat == True and geopy.distance.geodesic(locs[locsgen.index(i[1])][1],nloc).meters < i[2] else False
            else:
                sat = True if sat == True and geopy.distance.geodesic(locs[locsgen.index(i[0])][1],nloc).meters < i[2] else False

    locs += [[tbg,nloc]]
    
    locsgen = [i[0] for i in locs]
    nongen = sorted([i for i in edges if (i[0] in locsgen and i[1] not in locsgen) or (i[0] not in locsgen and i[1] in locsgen)], key = lambda x: x[2])
    if len(nongen) > 0:
        return GenerateDynGeo(edges,locs, nongen[0][0] if nongen[0][0] not in locsgen else nongen[0][1])
    else:
        disgen = [i for i in edges if (i[0] not in locsgen and i[1] not in locsgen)]
        if len(disgen) > 0:
            dtbg = disgen[0][1]
            locs += [[disgen[0][0],locs[0][1]]]
            return GenerateDynGeo(edges, locs, dtbg)
    return locs

def GenerateDynDiff(edges, tms, tbg):
    """An algorithm that determines integers that satisfy the constraints of the graph"""
    tmsgen = [i[0] for i in tms]
    subedges = [i for i in edges if (i[0] == tbg or i[1] == tbg) and (i[0] in tmsgen or i[1] in tmsgen)]
    invcond = False
    if subedges[0][0] == tbg:
        tm = tms[tmsgen.index(subedges[0][1])]
    else:
        tm = tms[tmsgen.index(subedges[0][0])]
        invcond = not invcond
    condis = subedges[0][2]
    ntm = None
    sat = False
    while(not(sat)):
        sat = True
        ntm = 0 if condis[0] == 0 else random.randint(condis[0],condis[1])
        for i in subedges:
            if i[0] == tbg:
                otm = tms[tmsgen.index(i[1])][1]
                sat = True if sat == True and otm+ntm >= otm+i[2][0] and otm+ntm <= otm+i[2][1] else False                
            else:
                otm = tms[tmsgen.index(i[0])][1]
                sat = True if sat == True and otm-ntm <= otm-i[2][0] and otm-ntm >= otm-i[2][1] else False
    tms += [[tbg, ntm+tm[1]]]

    tmsgen = [i[0] for i in tms]
    nongen = sorted([i for i in edges if (i[0] in tmsgen and i[1] not in tmsgen) or (i[0] not in tmsgen and i[1] in tmsgen)], key = lambda x: x[2])
    if len(nongen) > 0:
        return GenerateDynDiff(edges,tms, nongen[0][0] if nongen[0][0] not in tmsgen else nongen[0][1])
    else:
        disgen = [i for i in edges if (i[0] not in tmsgen and i[1] not in tmsgen)]
        if len(disgen) > 0:
            dtbg = disgen[0][1]
            tms += [[disgen[0][0],tms[0][1]]]
            return GenerateDynDiff(edges, tms, dtbg)
    return tms
    
class AttributeGen:
    """A class which provides methods of generating attribute values"""
    def __init__(self, wrldir, attrcounts = None, num_threads=1):
        self.attrcounts = {}
        if attrcounts != None:
            self.attrcounts = attrcounts
        else:
            self.ReadWorld(wrldir)

    def ReadWorld(self, wrldir, exts = ['nt', 'gz']):
        """Function to read the ntriples in a world graph directory and determine attribute counts"""
        for i in os.listdir(wrldir):
            fpath = os.path.join(wrldir, i)
            fformat = i.split('.')[-1]
            if os.path.isfile(fpath) and fformat in exts:
                g = rdflib.Graph()
                if fformat != 'gz':
                    with open(fpath) as f:
                        g.parse(f, format=fformat)
                elif fformat == 'gz':
                    with gzip.open(fpath) as f:
                        g.parse(f, format=i.split('.')[-2])
                for j in g:
                    if type(j[2]) != rdflib.term.URIRef:
                        attr = j[1].split(baseURI)[-1]
                        attrval = j[2].toPython()
                        try:
                            attrval = float(attrval)
                        except:
                            attrval = attrval
                        if attr not in self.attrcounts.keys():
                            self.attrcounts[attr] = {}
                        if attrval in self.attrcounts[attr].keys():
                            self.attrcounts[attr][attrval] += 1
                        else:
                            self.attrcounts[attr][attrval] = 1

    def CheckIterable(self, val):
        """Function to check if a variable is an iterable"""
        try:
            iter(val)
            return True
        except:
            return False

    def CheckFloat(self, val):
        """Function to check if a variable is a float"""
        try:
            float(val)
            return True
        except:
            return False

    def GenerateAttribute(self, attr, cond, condval, difficulty=0.5): #difficulty is from 0 to 1 where 0 is easier
        """Function to generate an attribute value that satisfies the constraint"""
        if cond == "EQUALS" or cond == "equals":
            return condval
        elif cond == "ANY" or cond == "any":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items()] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return RanString()
        elif cond == "STRING_CONTAINS" or cond == "stringLike":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items() if self.CheckIterable(k) and condval in k.split(" ")] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return RanString() + " " + condval + " " + RanString()
        elif cond == "LESS_THAN" or cond == "lessThan":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items() if self.CheckFloat(k) and k < float(condval)] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return random.uniform(0, float(condval) - SigDig(float(condval)))
        elif cond == "LESS_THAN_OR_EQUAL" or cond == "lessThanOrEquals":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items() if self.CheckFloat(k) and k <= float(condval)] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return random.uniform(0, float(condval))
        elif cond == "GREATER_THAN" or cond == "greaterThan":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items() if self.CheckFloat(k) and k > float(condval)] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return random.uniform(float(condval) + SigDig(float(condval)), float(condval) + SigDig(float(condval))*2)
        elif cond == "GREATER_THAN_OR_EQUAL" or cond == "greaterThanOrEquals":
            candidates = [(k, v) for k, v in self.attrcounts[attr].items() if self.CheckFloat(k) and k >= float(condval)] if attr in self.attrcounts.keys() else []
            if len(candidates) > 0:
                candidates = sorted(candidates, key=lambda x: x[1])
                #idx = math.floor(len(candidates)*difficulty)
                return random.choice(candidates)[0]
            else:
                return random.uniform(float(condval), float(condval) + SigDig(float(condval)))
        else:
            return None

class TemplateMerger:
    """Template merger for use with templates in V0 template schema"""
    def __init__(self, t1, t2):
        logger.debug('t1: '+t1+'   t2: '+t2)
        self.t1 = json.load(open(t1))
        self.t2 = json.load(open(t2))
        self.template = {}

    def DirectMerge(self):
        """A function that attempts to directly merge two templates together, templates must have identical keys"""
        template = {}
        if sorted(self.t1.keys()) == sorted(self.t2.keys()):
            for k,v in self.t1.items():
                if type(v) == str:
                    template[k] = v + "_" + self.t2[k]
                elif type(v) == list:
                    template[k] = v + self.t2[k]
        else:
            print("Template keys do not match, direct merge not possible")
            
        self.template = template
        return self

    def MixMerge(self):
        """A function that attempts to directly merge two templates together, templates do not need to have same keys"""
        template = {}
        for k,v in self.t1.items():
            if type(v) == str:
                template[k] = v + "_" + self.t2[k] if k in self.t2.keys() else v
            elif type(v) == list:
                template[k] = v + self.t2[k] if k in self.t2.keys() else v
                
        for k,v in self.t2.items():
            if k not in template.keys():
                if type(v) == str:
                    template[k] = v
                elif type(v) == list:
                    template[k] = v
                    
        self.template = template
        return self

    def AddComparisonConstraints(self):
        """Function that adds a geoconstraint between the merged templates to create a connected graph"""
        atemplate = None
        btemplate = None
        if "comparisonConstraints" in self.template.keys():
            if "comparisonConstraints" in self.t1.keys():
                atemplate = self.t1
                btemplate = self.t2
            else:
                atemplate = self.t2
                btemplate= self.t1
        else:
            print("No comparisonConstraints defined, did you merge the templates?")

        aGEO = copy.deepcopy([i for i in atemplate['comparisonConstraints'] if "norm" in i['constraint'].keys() and i['constraint']['norm'] == "GEO_DISTANCE"])
        logger.debug('len ageo: '+str((len(aGEO)))+'   type ageo: '+str(type(aGEO)))
        if len(aGEO) == 0:
            return self
        ranGEO = lambda: aGEO[random.randint(0, len(aGEO)-1)]
        
        nconstraints = []
        for i in btemplate['thingSpecs']:
            if "Event" in i['schemaClass']:
                nGEO = ranGEO()
                locationID = None
                for j in btemplate['relationshipSpecs']:
                    if j['node1'] == i['id'] and j['edge'] == 'location':
                        locationID = j['node2']
                    elif j['node2'] == i['id'] and j['edge'] == 'location':
                        locationID = j['node1']
                if locationID == None:
                    locationID = RanString()
                    self.template['thingSpecs'] += [{
                        "id": locationID,
                        "schemaClass": "Location",
                        "importance": 0,
                        "prose": "SignalGen Merge Location"
                    }]
                    self.template['relationshipSpecs'] += [{
                        "node1": i['id'],
                        "edge": "location",
                        "node2": locationID,
                        "importance": 1
                    }]
                nGEO['thing2'] = locationID
                nconstraints += [nGEO]
        self.template['comparisonConstraints'] += nconstraints
        return self
    
    def WriteTemplate(self, fwrite):
        """Function to write template out to file"""
        with open(fwrite, 'w') as f:
            f.write(json.dumps(self.template, indent=2))
        return self       

    
class AttrFaker:
    """Class that generates fake attribute values for a subset of attributes"""
    def __init__(self, lat, lon, seed):
        self.hash_map = {}
        self.lat = float(lat)
        self.lon = float(lon)
        self.seed = seed

    def GenerateFakes(self, dataSchemaClass, curAttrs):
        """Function that attempts to generate a fake value"""
        nAttrs = []
        if dataSchemaClass == 'PersonIdentifier':
            nAttrs = self.PersonIdentifier()
        if dataSchemaClass == 'AddressLocation':
            nAttrs = self.AddressLocation()

        retAttrs = []
        for i in nAttrs:
            found = False
            for j in curAttrs:
                if i[0] == j[0] and i[2] == j[2]:
                    found = True
#                    if j[1] in self.hash_map.keys():
#                        j[1] = self.hash_map[j[1]]
#                    else:
#                        j[1] = i[1]
#                        self.hash_map[j[1]] = i[1]
            if found == False:
                retAttrs += [i]
        return retAttrs

    def PersonIdentifier(self):
        """Function to generate a fake person name"""
        fake = Faker()
        Faker.seed(self.seed)
        return [['personIdentifierSurName', fake.last_name(), 'value'],
                ['personIdentifierGivenName', fake.first_name(), 'value']
                ]

    def AddressLocation(self):
        """Function to generate a fake address location, uses the nominatim service to find street address information"""
        latlon = RandLatLon(self.lat, self.lon, 10000)
        rlookup = schema_grapher.util.LatLonQuery(latlon[0], latlon[1])
        qdict = rlookup[0]['features'][0]
        keys = qdict['properties'].keys()
        if 'error' in keys:
            return []
        akeys = qdict['properties']['address'].keys() if type(qdict['properties']['address']) is dict else {}
        triples = []
        if 'display_name' in keys and qdict["properties"]['display_name'] != "":
            triples += [["locationAddressFullText", qdict["properties"]['display_name'], 'value']]
        if 'city' in akeys:
            triples += [["locationCity", qdict["properties"]['address']['city'], 'value']]
        if 'country' in akeys:
            triples += [["locationCountry", qdict["properties"]['address']['country'], 'value']]
        if 'county' in akeys:
            triples += [["locationCounty", qdict["properties"]['address']['county'], 'value']]
        if 'postcode' in akeys:
            triples += [["locationPostalCode", qdict["properties"]['address']['postcode'], 'value']]
        if 'state' in akeys:
            triples += [["locationState", qdict["properties"]['address']['state'], 'value']]
        if 'road' in akeys:
            triples += [["locationStreet", qdict["properties"]['address']['road'], 'value']]
        if 'house_number' in akeys:
            triples += [["locationStreetNumberText", qdict["properties"]['address']['house_number'], 'value']]

        if 'geometry' in qdict.keys():
            triples += [["locationGeoPoint", latlon, 'geojson']]
        
        return triples
