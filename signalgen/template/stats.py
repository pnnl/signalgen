import json
import urllib

RDFNS = 'http://schema.localhost/'
class Stats(dict):
    """Class that inherits from the dictionary class and mimics structure of attribute count dictionary; uses stats endpoint to lazy fill out the dictionary as queries are made to dictionary"""
    def __init__(self, schema, qapi = 'http://api.localhost/query-dev/'):
        self.qapi = qapi
        super().__init__()
        self.PropertyType(schema)

    def PropertyType(self, schema_jsonld):
        """Function used to determine the type of an schema attribute"""
        pt = {}
        schema = None
        if 'http://' in schema_jsonld:
            try:
                qr = urllib.request.urlopen(schema_jsonld)
                schema = json.loads(qr.read().decode())
            except Exception as e:
                logger.error("Could not retrieve schema from external server", exc_info=e)
        elif os.path.exists(schema_jsonld):
            with open(schema_jsonld) as f:
                schema = json.load(f)
        if type(schema) is dict and '@graph' in schema.keys():
            pt = {i['@id'].replace(RDFNS, '') : i[RDFNS + 'domainIncludes']['@id'].replace(RDFNS, '') for i in schema['@graph'] if RDFNS + 'rangeIncludes' in i.keys()}
        self.pt = pt

    def qapiPropertyCounts(self, prop, size=1000):
        """Function to query the stats endpoint for the attribute counts, by default returns the 1000 most frequent attributes"""
        ntype = self.pt[prop]
        post = {
            "nodeProperties" : [
                {
                    "nodeType" : ntype, "propertyName" : prop
                }
            ]
        }
        data = json.dumps(post).encode('utf-8')
        req =  urllib.request.Request(self.qapi + 'stats/propertyCounts?size=' + size, data=data) # this will make the method "POST"
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        resp = urllib.request.urlopen(req)
        respdata = json.load(resp)
        self.__dict__[prop] = respdata['propertyBuckets'][0]['buckets']
        return self

    #The following is boilerplate code for a dictionary with the exception of getitem which calls the stats endpoint if a key isn't in it's dictionary
    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        if key not in self.__dict__.keys():
            self.qapiPropertyCounts(key)
        return self.__dict__[key]

    def __repr__(self):
        return repr(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __delitem__(self, key):
        del self.__dict__[key]

    def clear(self):
        return self.__dict__.clear()

    def copy(self):
        return self.__dict__.copy()

    def has_key(self, k):
        return k in self.__dict__

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def pop(self, *args):
        return self.__dict__.pop(*args)

    def __cmp__(self, dict_):
        return self.__cmp__(self.__dict__, dict_)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __unicode__(self):
        return unicode(repr(self.__dict__))
