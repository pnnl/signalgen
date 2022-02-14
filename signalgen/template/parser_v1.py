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

from signalgen.template import *

baseURI = 'http://schema.localhost/'


class TemplateParserV1:
    """Class to parse V1.0 and V1.2 templates and generate signals"""
    TEMPLATE_VERSION = ['V1.0', 'V1.2']
    def __init__(self, jfile, cdir, pt, lat, lon, seed, initdate, difficulty=0.5, attributegen=None, partial=None):
        jtemp = None
        if type(jfile) is str and os.path.isfile(jfile):
            with open(jfile) as f:
                jtemp = json.load(f)
        else:
            jtemp = jfile

        self.cdir = cdir
        self.template = jtemp
        self.pt = pt
        self.lat = float(lat)
        self.lon = float(lon)
        self.initdate = initdate
        self.seed = seed
        self.originSource = self.template["id"] + "|" + str(difficulty) + "|" + str(seed) + "." + str(initdate)
        self.rdf = []
        self.difficulty=difficulty
        self.AttributeGen = self.GenerateAttribute if attributegen == None else attributegen
        self.partial = partial
        self.subtemplates = []
        self.components = {}

    def LoadComponents(self):
        """Function that reads components from a directory"""
        for i in self.cdir:
            for j in os.listdir(i):
                fullpath = os.path.join(i,j)
                if fullpath.split('.')[-1] == 'json':
                    with open(fullpath) as f:
                        component = json.load(f)
                        self.components[component['id']] = component
        return self

    def MergeHydratedToTemplate(self):
        """Function that copies template components into main template"""
        for i,ix in enumerate(self.template['templateComponents']):
            ix['hydratedThingSpec'] = copy.deepcopy(self.components[ix['id']]['hydratedThingSpec'])
            if 'thingPropertyConstraintOverrides' in ix.keys():
                for j in ix['thingPropertyConstraintOverrides']:
                    if j['pathToThing'] == 'root':
                        ix['hydratedThingSpec']['rootThing']['dataTypePropertyConstraints'] = j['dataTypePropertyConstraints']
                    else:
                        for k in ix['hydratedThingSpec']['thingPropertyConstraints']:
                            if k['pathFromRoot'] == j['pathToThing'].lstrip('root.'):
                                k['dataTypePropertyConstraints'] = j['dataTypePropertyConstraints']
        return self
        
    def GenerateThings(self):
        """Function that generates values for things"""
        things = []
        for i,ix in enumerate(self.template["templateComponents"]):
            ix['hydratedThingSpec']['rootThing']["value"] = str(uuid.UUID(hashlib.md5((self.originSource + json.dumps(ix) + str(random.random())).encode("utf-8")).hexdigest()))
            ix['hydratedThingSpec']['rootThing']["generated"] = True
            ix['hydratedThingSpec']['rootThing']["attributes"] = []
            if "dataTypePropertyConstraints" in ix['hydratedThingSpec']['rootThing']:
                ix['hydratedThingSpec']['rootThing']["attributes"] = self.GenerateDataTypePropertyConstraint(ix['hydratedThingSpec']['rootThing'])
            for k,kx in enumerate(ix['hydratedThingSpec']['thingPropertyConstraints']):
                if "generated" not in kx.keys():
                    kx["value"] = str(uuid.UUID(hashlib.md5((self.originSource + json.dumps(kx) + str(random.random())).encode("utf-8")).hexdigest()))
                    kx["generated"] = True
                    kx["attributes"] = []
                    if "dataTypePropertyConstraints" in kx:
                        kx["attributes"] = self.GenerateDataTypePropertyConstraint(kx)
        return self

    #currently ignoring constraint values after index 1
    def GenerateDataTypePropertyConstraint(self, kx):
        """Function that satisfies the attribute constraints on a specific thing"""
        attributes = []
        nval = {}
        for j in kx["dataTypePropertyConstraints"]:
            if 'equals' in j.keys():
                for k in j['equals']:
                    kvals = list(k.values())[0]
                    kkeys = list(k.keys())
                    nval[kvals[0]] = self.AttributeGen(kvals[0], kkeys[0], kvals[1], self.difficulty)
            elif 'and' in j.keys():
                for k in j['and']:
                    kvals = list(k.values())[0]
                    kkeys = list(k.keys())
                    if kvals[0] not in tuple(nval.keys()):
                        nval[kvals[0]] = self.AttributeGen(kvals[0], kkeys[0], kvals[1], self.difficulty)
                    else:
                        nval[kvals[0]] = nval[kvals[0]] + self.AttributeGen(kvals[0], kkeys[0], kvals[1], self.difficulty)
                
            elif 'or' in j.keys():
                for k in j['or']:
                    kvals = list(k.values())[0]
                    kkeys = list(k.keys())
                    if kvals[0] not in tuple(nval.keys()):
                        nval[kvals[0]] = self.AttributeGen(kvals[0], kkeys[0], kvals[1], self.difficulty)
                    else:
                        nval[kvals[0]] = nval[kvals[0]] + self.AttributeGen(kvals[0], kkeys[0], kvals[1], self.difficulty)
            else:
                jkeys = list(j.keys())
                jvals = list(j.values())[0] if type(j[jkeys[0]]) is not dict else [j[jkeys[0]]['leftHandSide'], j[jkeys[0]]['rightHandSide']['value']]
                nval[jvals[0]] = self.AttributeGen(jvals[0], jkeys[0], jvals[1], self.difficulty)
        for k,v in nval.items():
            attributes += [[k, v, 'value']]
        return attributes

    def GenerateAttribute(self, attr, cond, condval, difficulty=0.5):
        """Default attribute generator function, generates random values that satisfy constraint"""
        attrValue = None
        if cond == "equals":
            attrValue = condval
        elif cond == "any":
            attrValue = RanString()
        elif cond == "stringLike":
            attrValue = RanString() + " " + condval + " " + RanString()
        elif cond == "lessThan":
            attrValue = random.uniform(0, float(condval) - SigDig(float(condval)))
        elif cond == "lessThanOrEquals":
            attrValue = random.uniform(0, float(condval))
        elif cond == "greaterThan":
            attrValue = random.uniform(float(condval) + SigDig(float(condval)), float(condval) + SigDig(float(condval))*2)
        elif cond == "greaterThanOrEquals":
            attrValue = random.uniform(float(condval), float(condval) + SigDig(float(condval)))
        return attrValue

    def GetValueFromPath(self, path):
        """Given a path resolves the referenced value"""
        pathcomp = path.split('.')
        for i in self.template['templateComponents']:
            if i['alias'] == pathcomp[0]:
                if pathcomp[1] == 'root':
                    if len(pathcomp) == 3:
                        return i['hydratedThingSpec']['rootThing']
                    else:
                        for j in i['hydratedThingSpec']['thingPropertyConstraints']:
                            if '.'.join(pathcomp[2:-1]) == j['pathFromRoot']:
                                return j
        return None

    def GetAttrFromThing(self, thing, attr):
        """Function to get a specific attributes value for a thing"""
        for i in thing['attributes']:
            if i[0] == attr:
                return i[1]
        return None

    def GenerateComparisons(self):
        """Fucntion that satisfies the comparison constraints in a template"""
        now = datetime.datetime.utcfromtimestamp(self.initdate)
        #logger.debug('Creating comparison constraints between nodes in the template.')
        if "templateComponentComparisonConstraints" not in self.template.keys():
            return self
        for i in self.template['templateComponentComparisonConstraints']:
            for k,v in i.items():
                if k == 'equals':
                    values = {}
                    attrs = []
                    for j in v:
                        values[j] = self.GetValueFromPath(j)
                    for jk, jv in values.items():
                        if len(jv['attributes']) > 0:
                            for a in jv['attributes']:
                                if a[0] == jk.split('.')[-1]:
                                    attrs += [a[1]]
                    if len(attrs) == 0:
                        attrs += [self.AttributeGen(list(values.keys())[0].split('.')[-1], 'any', '', self.difficulty)]
                    for jk, jv in values.items():
                        attrset = None
                        if len(jv['attributes']) > 0:
                            for a in jv['attributes']:
                                if a[0] == jk.split('.')[-1]:
                                    attrset = True
                        if attrset == None:
                            jv['attributes'] += [[jk.split('.')[-1], attrs[0], 'value']]
                if k == 'difference':
                    subtrahend = self.GetValueFromPath(v['subtrahend'])
                    minuend = self.GetValueFromPath(v['minuend'])
                    schemaAttribute1 = v['subtrahend'].split('.')[-1]
                    schemaAttribute2 = v['minuend'].split('.')[-1]
                    aval = self.GetAttrFromThing(subtrahend, schemaAttribute1)
                    bval = self.GetAttrFromThing(minuend, schemaAttribute2)
                    cval = 0
                    if "minValue" in v.keys():
                        cval = int(v["minValue"])
                        if "maxValue" in v.keys():
                            if 0 >= int(v["minValue"]) and 0 <= int(v["maxValue"]):
                                cval = 0

                    if aval != None and aval == bval:
                        None
                    elif aval == None and bval == None:
                        nval = self.AttributeGen(schemaAttribute1, "ANY", "", self.difficulty)
                        if type(nval) == str:
                            nval = now
                        xattr = [schemaAttribute1, self.AttributeGen(schemaAttribute1, "EQUALS", nval - datetime.timedelta(days=cval), self.difficulty), 'value']
                        yattr = [schemaAttribute2, self.AttributeGen(schemaAttribute2, "EQUALS", nval + datetime.timedelta(days=0), self.difficulty), 'value']
                        subtrahend["attributes"] += [xattr]
                        minuend["attributes"] += [yattr]
                    elif aval != None and bval == None:
                        nval = aval + datetime.timedelta(days=cval)
                        attr = [schemaAttribute2, nval, 'value']
                        minuend["attributes"] += [attr]
                    elif aval == None and bval != None:
                        nval = bval - datetime.timedelta(days=cval)
                        attr = [schemaAttribute1, nval, 'value']
                        subtrahend["attributes"] += [attr]
                if k == "geoNear":
                    first = self.GetValueFromPath(v['geometries'][0])
                    second = self.GetValueFromPath(v['geometries'][1])
                    schemaAttribute1 = v['geometries'][0].split('.')[-1]
                    schemaAttribute2 = v['geometries'][1].split('.')[-1]
                    xattr = self.GetAttrFromThing(first, schemaAttribute1)
                    yattr = self.GetAttrFromThing(second, schemaAttribute2)
                    if xattr == None and yattr == None:
                        first["attributes"] += [[schemaAttribute1, RandLatLon(self.lat, self.lon, 500), 'geojson']]
                        xattr = AttrIndex(first["attributes"], schemaAttribute1)
                    if xattr != None:
                        xlat = xattr[1][0]
                        xlon = xattr[1][1]
                        nlatlon = RandLatLon(xlat, xlon, v["distance"])
                        distance = geopy.distance.geodesic([xlat, xlon],nlatlon).meters
                        second["attributes"] += [[schemaAttribute2, nlatlon, 'geojson']]
                        if distance > v["distance"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                    elif yattr != None:
                        ylat = yattr[1][0]
                        ylon = yattr[1][1]
                        nlatlon = RandLatLon(ylat, ylon, v["distance"])
                        distance = geopy.distance.geodesic([ylat, ylon],nlatlon).meters
                        first["attributes"] += [[schemaAttribute2, nlatlon, 'geojson']]
                        if distance > v["distance"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                    else:
                        xlat = xattr[1][0]
                        xlon = xattr[1][1]
                        ylat = yattr[1][0]
                        ylon = yattr[1][1]
                        distance = geopy.distance.geodesic([xlat, xlon],[ylat, ylon]).meters
                        if distance > v["distance"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                if k == "sameAsNode":
                    first = self.GetValueFromPath(v[0])
                    second = self.GetValueFromPath(v[1])
                    schemaAttribute1 = v[0].split('.')[-1]
                    schemaAttribute2 = v[1].split('.')[-1]
                    aval = self.GetAttrFromThing(first, schemaAttribute1)
                    bval = self.GetAttrFromThing(second, schemaAttribute2)
                    second['value'] = first['value']
            i['generated'] = True
            
        return self

    def GenerateFakes(self):
        """Function that will generate fake values for unconstrained things"""
        faker = AttrFaker(self.lat, self.lon, self.seed)
        for i in self.template["templateComponents"]:
            things = [i['hydratedThingSpec']['rootThing']] + i['hydratedThingSpec']['thingPropertyConstraints']
            for j in things:
                j['attributes'] += faker.GenerateFakes(j['dataSchemaClass'], [k for k in j['attributes'] if k[2] != 'object'])
        return self


    def GenerateRDF(self, trim = False):
        """Function to generate triples list from template"""
        components = self.template["templateComponents"]
        if self.partial != None:
            if self.partial["MODE"] == "COMPONENT":
                components = random.sample(components, int(len(components) * self.partial["PERCENT"]/100))
        for i in components:
            empty = []
            i['hydratedThingSpec']['rootThing']['attributes'] += [[j["pathFromRoot"], j["value"], "object"] for j in i['hydratedThingSpec']['thingPropertyConstraints'] if '.' not in j['pathFromRoot']]
            things = [i['hydratedThingSpec']['rootThing']] + i['hydratedThingSpec']['thingPropertyConstraints']
            if self.partial != None:
                if self.partial["MODE"] == "NODE":
                    things = random.sample(things, int(len(things) * self.partial["PERCENT"]/100))
            for j in things:
                j['attributes'] += [[k["pathFromRoot"].split('.')[-1], k["value"], "object"] for k in i['hydratedThingSpec']['thingPropertyConstraints'] if 'pathFromRoot' in j.keys() and '.'.join(k['pathFromRoot'].split('.')[0:-1]) == j['pathFromRoot']]
                dedup = set(tuple(tuple(y) if type(y) is list else y for y in x) for x in j["attributes"])
                if len(dedup) > 0:
                    self.rdf += [(schema_grapher.util.WrapNS(j["value"]), '<' + schema_grapher.util.RDFTYPE + '>', schema_grapher.util.WrapNS(j["dataSchemaClass"]))]
                    self.rdf += [(schema_grapher.util.WrapNS(j["value"]), schema_grapher.util.WrapNS("metaData"), schema_grapher.util.ParseDatum("metaData", json.dumps({"answerKey" : str(self.originSource) + '|' + ".".join([i["alias"],"root"] + ([j["pathFromRoot"]] if "pathFromRoot" in j.keys() else []))}), self.pt))]
                    for k in dedup:
                        if k[2] == 'object':
                            self.rdf += [(schema_grapher.util.WrapNS(j["value"]), schema_grapher.util.WrapNS(k[0]), schema_grapher.util.WrapNS(k[1]))]
                        elif k[2] == 'geojson':
                            self.rdf += [(schema_grapher.util.WrapNS(j["value"]), schema_grapher.util.WrapNS(k[0]), schema_grapher.util.ParseDatum(k[0], {"type": "Point", "coordinates": [float(k[1][1]), float(k[1][0])]}, self.pt))]
                        else:
                            self.rdf += [(schema_grapher.util.WrapNS(j["value"]), schema_grapher.util.WrapNS(k[0]), schema_grapher.util.ParseDatum(k[0], k[1], self.pt))]
                elif not trim:
                    empty += [(schema_grapher.util.WrapNS(j["value"]), '<' + schema_grapher.util.RDFTYPE + '>', schema_grapher.util.WrapNS(j["dataSchemaClass"]))]
                    empty += [(schema_grapher.util.WrapNS(j["value"]), schema_grapher.util.WrapNS("metaData"), schema_grapher.util.ParseDatum("metaData", json.dumps({"answerKey" : str(self.originSource) + '|' + ".".join([i["alias"],"root"] + ([j["pathFromRoot"]] if "pathFromRoot" in j.keys() else []))}), self.pt))]
            self.rdf += empty
        return self

    def WriteTemplate(self, fwrite):
        """Function to write template object to file, useful for inspecting template as it is satified"""
        with open(fwrite, 'w') as f:
            f.write(json.dumps(self.template, indent=2))
        return self
    
    def WriteTriples(self, fwrite):
        """Function to write out n-triples to a file"""
        with open(fwrite, 'w') as f:
            f.write(schema_grapher.util.RenderTriples(self.rdf))
        return self
