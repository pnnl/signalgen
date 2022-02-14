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

from signalgen.template.utils import *

baseURI = 'http://schema.localhost/'

class TemplateParser:
    """Class to parse V0 templates and generate signals"""
    TEMPLATE_VERSION = ['V0']
    def __init__(self, jfile, pt, lat, lon, seed, initdate, difficulty=0.5, attributegen=None):
        jtemp = None
        if type(jfile) is str and os.path.isfile(jfile):
            with open(jfile) as f:
                jtemp = json.load(f)
        else:
            jtemp = jfile
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
        self.subtemplates = []

    def GenerateSubTemplates(self):
        """Function that instantiates template parsers for each subtemplate"""
        if "optionSubTemplateSpecs" in self.template.keys():
            for i in self.template["optionSubTemplateSpecs"]:
                self.subtemplates += [TemplateParser(i["template"], self.lat, self.lon, self.seed, self.initdate+1, self.difficulty, self.AttributeGen).GenerateSubTemplates().ResolveIdentities().GenerateThings().GenerateRelationships().GenerateComparisons()]
        return self

    def ResolveIdentities(self):
        """Function to resolve referenced things between main template and subtemplates"""
        identities = {}
        if "thingIdentities" in self.template.keys():
            for i in self.template["thingIdentities"]:
                if i["thingSpecId1"] not in identities.keys():
                    identities[i["thingSpecId1"]] = []
                identities[i["thingSpecId1"]] += [i["thingSpecId2"]]
            for k,v in identities.items():
                choice = random.choice(v)
                resolved = False
                for i in self.subtemplates:
                    for j in i.template["thingSpecs"]:
                        if choice == j["id"] and not resolved:
                            for l in range(len(self.template["thingSpecs"])):
                                if k == self.template["thingSpecs"][l]["id"]:
                                    del self.template["thingSpecs"][l]
                                    break
                            if "relationshipSpecs" in self.template.keys():
                                for l in range(len(self.template["relationshipSpecs"])):
                                    if self.template["relationshipSpecs"][l]["node1"] == k:
                                        self.template["relationshipSpecs"][l]["node1"] = choice
                                    if self.template["relationshipSpecs"][l]["node2"] == k:
                                        self.template["relationshipSpecs"][l]["node2"] = choice
                            if "comparisonConstraints" in self.template.keys():
                                for l in range(len(self.template["comparisonConstraints"])):
                                    if self.template["comparisonConstraints"][l]["thing1"] == k:
                                        self.template["comparisonConstraints"][l]["thing1"] = choice
                                    if self.template["comparisonConstraints"][l]["thing2"] == k:
                                        self.template["comparisonConstraints"][l]["thing2"] = choice
                            self.template["thingSpecs"] += i.template["thingSpecs"]
                            if "relationshipSpecs" in self.template.keys() and "relationshipSpecs" in i.template.keys():
                                self.template["relationshipSpecs"] += i.template["relationshipSpecs"]
                            if "comparisonConstraints" in self.template.keys() and  "comparisonConstraints" in i.template.keys():
                                self.template["comparisonConstraints"] += i.template["comparisonConstraints"]
                            resolved = True
                            
        return self
        
    def GenerateThings(self):
        """Function that generates values for things"""
        things = []
        for i in range(len(self.template["thingSpecs"])):
            if "generated" not in self.template["thingSpecs"][i]:
                self.template["thingSpecs"][i]["value"] = str(uuid.UUID(hashlib.md5((self.originSource + json.dumps(self.template["thingSpecs"][i]) + str(random.random())).encode("utf-8")).hexdigest()))
                self.template["thingSpecs"][i]["generated"] = True
                self.template["thingSpecs"][i]["attributes"] = []            
                if "attributeConstraints" in self.template["thingSpecs"][i]:
                    nval = {}
                    for j in self.template["thingSpecs"][i]["attributeConstraints"]:
                        if j["schemaAttribute"] not in nval.keys():
                            nval[j["schemaAttribute"]] = self.AttributeGen(j["schemaAttribute"], j["attributeConstraintType"], j["value"], self.difficulty)
                        else:
                            nval[j["schemaAttribute"]] = nval[j["schemaAttribute"]] + self.AttributeGen(j["schemaAttribute"], j["attributeConstraintType"], j["value"], self.difficulty)
                    for k,v in nval.items():
                        self.template["thingSpecs"][i]["attributes"] += [[k, v, 'value']]
        return self

    def GenerateAttribute(self, attr, cond, condval, difficulty=0.5):
        """Default attribute generator function, generates random values that satisfy constraint"""
        attrValue = None
        if cond == "EQUALS":
            attrValue = condval
        elif cond == "ANY":
            attrValue = RanString()
        elif cond == "STRING_CONTAINS":
            attrValue = RanString() + " " + condval + " " + RanString()
        elif cond == "LESS_THAN":
            attrValue = random.uniform(0, float(condval) - SigDig(float(condval)))
        elif cond == "LESS_THAN_OR_EQUAL":
            attrValue = random.uniform(0, float(condval))
        elif cond == "GREATER_THAN":
            attrValue = random.uniform(float(condval) + SigDig(float(condval)), float(condval) + SigDig(float(condval))*2)
        elif cond == "GREATER_THAN_OR_EQUAL":
            attrValue = random.uniform(float(condval), float(condval) + SigDig(float(condval)))
        return attrValue

    def GenerateRelationships(self):
        """Function to satisfy relationship specs in template"""
        if "relationshipSpecs" in self.template.keys():
            for j,i in enumerate(self.template["relationshipSpecs"]):
                if "generated" not in i.keys():
                    x = DictIndex(self.template["thingSpecs"], "id", i["node1"])
                    y = DictIndex(self.template["thingSpecs"], "id", i["node2"])
                    self.template["thingSpecs"][x]["attributes"] += [[i["edge"], self.template["thingSpecs"][y]["value"], 'object']]
                    self.template["relationshipSpecs"][j]["generated"] = True
        return self

    def GenerateComparisons(self, simdiff = False, simgeo=True, presortConstraints=False, thing=None):
        """Function to satisfy comparison constraints in template"""
        #logger.debug('Creating comparison constraints between nodes in the template.')
        if "comparisonConstraints" not in self.template.keys():
            return self
        if presortConstraints:
            self.template["comparisonConstraints"] = sorted(self.template["comparisonConstraints"], reverse=True, key=lambda x: x['constraint']['minValue'] if 'minValue' in x['constraint'].keys() else 0)
        if simgeo:
            edges = []
            for j,i in enumerate(self.template["comparisonConstraints"]):
                if "norm" in i["constraint"].keys() and i["constraint"]["norm"] == "GEO_DISTANCE":
                    edges += [[i["thing1"],i["thing2"],i["constraint"]["maxValue"]]]
            if len(edges) > 0:
                edges = sorted(edges, key = lambda x: x[0])
                locs = GenerateDynGeo(edges, [[edges[0][0], RandLatLon(self.lat, self.lon, 500)]], edges[0][1])
                locsdict = {i[0]:i[1] for i in locs}
        if simdiff:
            edges = []
            for j,i in enumerate(self.template["comparisonConstraints"]):
                if "differenceConstraint" in i["constraint"].keys():
                    edges += [[i["thing1"],i["thing2"],(i["constraint"]["minValue"], i["constraint"]["maxValue"])]]
            if len(edges) > 0:
                edges = sorted(edges, key = lambda x: x[0])
                tms = GenerateDynDiff(edges, [[edges[0][0], 0]], edges[0][1])
                tmsdict = {i[0]:i[1] for i in tms}
        now = datetime.datetime.utcfromtimestamp(self.initdate)
        for j,i in enumerate(self.template["comparisonConstraints"]):
            if "generated" not in self.template["comparisonConstraints"][j].keys():
                x = DictIndex(self.template["thingSpecs"], "id", i["thing1"])
                y = DictIndex(self.template["thingSpecs"], "id", i["thing2"])
                xattr = AttrIndex(self.template["thingSpecs"][x]["attributes"], i["schemaAttribute1"])
                yattr = AttrIndex(self.template["thingSpecs"][y]["attributes"], i["schemaAttribute2"])
                if thing != None and i["thing1"] != thing and i["thing2"] != thing:
                    continue
                if "predicate" in i["constraint"].keys() and i["constraint"]["predicate"] == "EQUALS":
                    aval = None
                    bval = None
                    for k,v in enumerate(self.template["thingSpecs"][x]["attributes"]):
                        if v[0] == i["schemaAttribute1"]:
                            aval = v[1]
                    for k,v in enumerate(self.template["thingSpecs"][y]["attributes"]):
                        if v[0] == i["schemaAttribute2"]:
                            bval = v[1]
                    if aval != None and aval == bval:
                        None
                    elif aval == None and bval == None:
                        nval = self.AttributeGen(i["schemaAttribute1"], "ANY", "", self.difficulty)
                        xattr = [i["schemaAttribute1"], self.AttributeGen(i["schemaAttribute1"], "EQUALS", nval, self.difficulty), 'value']
                        yattr = [i["schemaAttribute2"], self.AttributeGen(i["schemaAttribute2"], "EQUALS", nval, self.difficulty), 'value']
                        self.template["thingSpecs"][x]["attributes"] += [xattr]
                        self.template["thingSpecs"][y]["attributes"] += [yattr]
                    elif aval != None and bval == None:
                        attr = [i["schemaAttribute2"], aval, 'value']
                        self.template["thingSpecs"][y]["attributes"] += [attr]
                    elif aval == None and bval != None:
                        attr = [i["schemaAttribute1"], bval, 'value']
                        self.template["thingSpecs"][x]["attributes"] += [attr]
                    self.template["comparisonConstraints"][j]["generated"] = True
                if "differenceConstraint" in i["constraint"].keys() and i["constraint"]["differenceConstraint"] == "true":
                    aval = None
                    bval = None
                    for k,v in enumerate(self.template["thingSpecs"][x]["attributes"]):
                        if v[0] == i["schemaAttribute1"]:
                            aval = v[1]
                    for k,v in enumerate(self.template["thingSpecs"][y]["attributes"]):
                        if v[0] == i["schemaAttribute2"]:
                            bval = v[1]

                    cval = 0
                    if "minValue" in i["constraint"].keys():
                        cval = int(i["constraint"]["minValue"])
                        if "maxValue" in i["constraint"].keys():
                            if 0 >= int(i["constraint"]["minValue"]) and 0 <= int(i["constraint"]["maxValue"]):
                                cval = 0

                    if aval != None and aval == bval:
                        None
                    elif aval == None and bval == None:
                        nval = self.AttributeGen(i["schemaAttribute1"], "ANY", "", self.difficulty)
                        if type(nval) == str:
                            nval = now
                        xattr = [i["schemaAttribute1"], self.AttributeGen(i["schemaAttribute1"], "EQUALS", nval + datetime.timedelta(days= (tmsdict[i["thing1"]] if simdiff else cval)), self.difficulty), 'value']
                        yattr = [i["schemaAttribute2"], self.AttributeGen(i["schemaAttribute2"], "EQUALS", nval + datetime.timedelta(days= (tmsdict[i["thing2"]] if simdiff else 0)), self.difficulty), 'value']
                        self.template["thingSpecs"][x]["attributes"] += [xattr]
                        self.template["thingSpecs"][y]["attributes"] += [yattr]
                    elif aval != None and bval == None:
                        nval = aval - datetime.timedelta(days=(tmsdict[i["thing1"]]-tmsdict[i["thing2"]] if simdiff else cval))
                        attr = [i["schemaAttribute2"], nval, 'value']
                        self.template["thingSpecs"][y]["attributes"] += [attr]
                    elif aval == None and bval != None:
                        nval = bval + datetime.timedelta(days=(tmsdict[i["thing1"]]-tmsdict[i["thing2"]] if simdiff else cval))
                        attr = [i["schemaAttribute1"], nval, 'value']
                        self.template["thingSpecs"][x]["attributes"] += [attr]
                    self.template["comparisonConstraints"][j]["generated"] = True
                if "norm" in i["constraint"].keys() and i["constraint"]["norm"] == "GEO_DISTANCE":
                    if xattr == None and yattr == None:
                        self.template["thingSpecs"][x]["attributes"] += [[i["schemaAttribute1"], locsdict[i["thing1"]] if simgeo else RandLatLon(self.lat, self.lon, 500), 'geojson']]
                        xattr = AttrIndex(self.template["thingSpecs"][x]["attributes"], i["schemaAttribute1"])
                    if xattr != None:
                        xlat = self.template["thingSpecs"][x]["attributes"][xattr][1][0]
                        xlon = self.template["thingSpecs"][x]["attributes"][xattr][1][1]
                        nlatlon = locsdict[i["thing2"]] if simgeo else RandLatLon(xlat, xlon, i["constraint"]["maxValue"])
                        distance = geopy.distance.geodesic([xlat, xlon],nlatlon).meters
                        self.template["thingSpecs"][y]["attributes"] += [[i["schemaAttribute2"], nlatlon, 'geojson']]
                        if distance > i["constraint"]["maxValue"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                    elif yattr != None:
                        ylat = self.template["thingSpecs"][y]["attributes"][yattr][1][0]
                        ylon = self.template["thingSpecs"][y]["attributes"][yattr][1][1]
                        nlatlon = locsdict[i["thing1"]] if simgeo else RandLatLon(ylat, ylon, i["constraint"]["maxValue"])
                        distance = geopy.distance.geodesic([ylat, ylon],nlatlon).meters
                        self.template["thingSpecs"][x]["attributes"] += [[i["schemaAttribute2"], nlatlon, 'geojson']]
                        if distance > i["constraint"]["maxValue"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                    else:
                        xlat = self.template["thingSpecs"][x]["attributes"][xattr][1][0]
                        xlon = self.template["thingSpecs"][x]["attributes"][xattr][1][1]
                        ylat = self.template["thingSpecs"][y]["attributes"][yattr][1][0]
                        ylon = self.template["thingSpecs"][y]["attributes"][yattr][1][1]
                        distance = geopy.distance.geodesic([xlat, xlon],[ylat, ylon]).meters
                        if distance > i["constraint"]["maxValue"]:
                            print("GEO_DISTANCE VIOLATED", x, y)
                    self.template["comparisonConstraints"][j]["generated"] = True
                if i["thing1"] == thing or thing == None:
                    self.GenerateComparisons(simdiff,simgeo,presortConstraints,i["thing1"])
                if i["thing2"] == thing or thing == None:
                    self.GenerateComparisons(simdiff,simgeo,presortConstraints,i["thing2"])
        return self


    def GenerateRDF(self, trim = False):
        """Function to generate triples list from template"""
        empty = []
        for i in self.template["thingSpecs"]:
            dedup = set(tuple(tuple(y) if type(y) is list else y for y in x) for x in i["attributes"])
            if len(dedup) > 0:
                self.rdf += [(schema_grapher.util.WrapNS(i["value"]), '<' + schema_grapher.util.RDFTYPE + '>', schema_grapher.util.WrapNS(i["schemaClass"]))]
                self.rdf += [(schema_grapher.util.WrapNS(i["value"]), schema_grapher.util.WrapNS("metaData"), schema_grapher.util.ParseDatum("metaData", json.dumps({"answerKey" : str(self.originSource) + '|' + i["id"]}), self.pt))]
                for j in dedup:
                    if j[2] == 'object':
                        self.rdf += [(schema_grapher.util.WrapNS(i["value"]), schema_grapher.util.WrapNS(j[0]), schema_grapher.util.WrapNS(j[1]))]
                    elif j[2] == 'geojson':
                        self.rdf += [(schema_grapher.util.WrapNS(i["value"]), schema_grapher.util.WrapNS(j[0]), schema_grapher.util.ParseDatum(j[0], {"type": "Point", "coordinates": [float(j[1][1]), float(j[1][0])]}, self.pt))]
                    else:
                        self.rdf += [(schema_grapher.util.WrapNS(i["value"]), schema_grapher.util.WrapNS(j[0]), schema_grapher.util.ParseDatum(j[0], j[1], self.pt))]
            elif not trim:
                empty += [(schema_grapher.util.WrapNS(i["value"]), '<' + schema_grapher.util.RDFTYPE + '>', schema_grapher.util.WrapNS(i["schemaClass"]))]
                empty += [(schema_grapher.util.WrapNS(i["value"]), schema_grapher.util.WrapNS("metaData"), schema_grapher.util.ParseDatum("metaData", json.dumps({"answerKey" : str(self.originSource) + '|' + i["id"]}), self.pt))]
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
