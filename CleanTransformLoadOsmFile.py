# Name : CleanTransformLoadOsmFile.Py 
# Created By : Tan Kwok Wee
# Purpose : Cleaning of OpenStreetMap Data - Calinfornia.osm, transform the xml into JSON and insert into database
# 1. Clean the XML Data
# 1.1 State Codes to be standardized to be CA or not included in the final JSON format
# 1.2 Abbreviated Street Names to be mapped to the correct 
# 1.3 Standardize postcode to be 5 digits
# 2. Transform the XML data into JSON format
# 3. load the JSON data into MongoDB
# 4. Write the JSON data into File

from datetime import datetime
import pprint
import json
import xml.etree.cElementTree as ET
import re
import codecs
from pymongo import MongoClient
from bson import json_util
from bson.objectid import ObjectId
from time import mktime


OSMFILE = "C:\\Users\\kw\\Documents\\Personal\\DataAnalyst_2015\\Project_2\\san-francisco.osm"
#OSMFILE = "C:\\Users\\212369277\\Documents\\Personal\\FY2014_DATAANALYST\\Project 2\\san-francisco.osm"
#OSMFILE = "C:\\Users\\212369277\\Documents\\Personal\\FY2014_DATAANALYST\\Project 2\\san-francisco-bay_california_sample.osm"

# regular expressions
state = re.compile(r'^\d{5}$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# Global Variables
STREET_NAMES_MAP = { "St": " Street",
            "St.": " Street",
            "Ave" : "Avenue",
            "Ave." : "Avenue",
            "Blvd" : "Boulevard",
            "Blvd." : "Boulevard",
            "Rd." : "Road",
            "Rd" : "Road"
            }

CREATED = [ "version", "changeset", "timestamp", "user", "uid", "type"]
ADDRESS = [ "housenumber", "postcode", "street","state"]
MAINATTRIB = ["id","type", "visible"]
SUBATTRIB = ["amenity","cuisine","name","phone","denomination","religion"]

class MyEncoder(json.JSONEncoder):
# class is to created to ensure the JSON datetime values are converted and can be successfully written into output file

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)


def clean_up_postcode(val) :
    
	# function is created to clean up post codes
	
    # if post code < 5 digits return empty string
    
    if state.search(val) :
	# if state contains 5 digits, return the value
	#	input "12345" will return "12345"
	
		return val
    elif len(val.split("-"))>1 :
    # if post code contains -, splits the postcode by "-"
	# if the first group of post code contains 5 digits return the value
	#	input "12345-001" will return "12345"
	#	input "1234-001" will return ""
	
        if state.search(val.split("-")[0]) :
            return val.split("-")[0]
        else :
            return ""
    elif len(val.split("CA "))>1 :
    # if post code contains CA, remove CA and return the remaining post code
	#	input "CA12345" will return "12345"
	#	input "CA1234" will return ""

		if state.search(val.split("CA ")[1]) :
            return val.split("CA ")[1]
        else :
            return ""
    else :
	# does not fulfill all the criteria then return empty string
        return ""
    
# cleanup logic for addr:state

def clean_up_state(val) :

	# function is created to clean up state codes
    
    incorrectstate = ["California","ca"]
    
    if val in incorrectstate :
	# if state value is either "California" or "ca", will return "CA"
        return "CA"
    elif val == "CA" :
	# if state value is CA then will return CA
        return val
    else :
	# else return empty string
        return ""
    
#cleanup logic for addr:street

def clean_up_street(name):

	# function is created to clean up street names
	
	# if the street names are found in the dictionary STREET_NAMES_MAP, then it will be replaced with the corresponding mapped value

    #print 'check', name
	# check every key in the STREET_NAMES_MAP against the street name provided
    for key in STREET_NAMES_MAP :
        #print 'search',key, name
        m = re.search(key+"$", name,flags=re.I)
        if m :
		# if the match is found, replace
		#	input "One Blvd." will return "One Boulevard"
		
            name = re.sub(pattern=key + "$" , repl=STREET_NAMES_MAP[key], string=name, flags=re.IGNORECASE)
            break

    return name


def shape_element(element):

	# function is created to extract required xml tag/attributes from open street map data and transform the extracted data into JSON format

    node = {}

    if element.tag == "node" or element.tag == "way" :
	# extract xml tags node or way 
		
        # YOUR CODE HERE
        #print element.tag, element.attrib
        
        
        node["type"] = element.tag
        
        for ma in MAINATTRIB :
		# check every required main attribute against the xml tag, if exists add to the final output
		
            if ma in element.attrib :
                node[ma] = element.get(ma)
    
        node["created"] = {}        
		
        for sac in CREATED :
		# check every required subset attribute "CREATED" against the xml tag, if exists add to the final output
		
            if sac in element.attrib :
                if sac == "timestamp" :
				# if attribute is timestamp, cast the value as datetime and format it as YYYY-MMM-DDTHH:MM:SS
                    node["created"][sac] = datetime.strptime(element.get(sac),"%Y-%m-%dT%H:%M:%SZ")
                else : 
				# else just add atrribute to the sub-node "created"
                    node["created"][sac] = element.get(sac)

        if "lat" in element.attrib and "lon" in element.attrib :
		# if attribute is lat or lon, then cast the value as float and convert them into a list
            node["pos"] = [ float(element.get("lat")) , float(element.get("lon"))]

        
        for subnode in element :
		# check every subnode in the element
		
            if element.tag == "way" :
			# if subnode tag is way
			
                if "ref" in subnode.attrib :
				# if subnode tag is way and ref is fgound in the attrib
				
                    if "node_refs" not in node :
					# node "node_refs" not created, initialize it
                        node["node_refs"] = []
                        
					# populate the node "node_refs"
                    node["node_refs"].append(subnode.get("ref"))
                    #print subnode.tag, subnode.attrib

            if "k" in subnode.attrib :
			# if "k" exists in the subnode attributes
                #print 'subnode', subnode.attrib
				# check that the value of the attributes does not contain problematic characters
                if not problemchars.search(subnode.get('k')) :      
                    
					# check if the attribute name contains addr
                    if re.search("addr",subnode.get("k")) :
						# check if the attribute name does not contain more than one ":"
                        if not len(re.findall(':', subnode.get("k"))) >= 2 :   
						
							# check all the valid address attributes we want to include, clean and transform
                            for addrec in ADDRESS :
                                if subnode.get("k").split(":") > 1 : 
                                    if addrec in subnode.get("k").split(":") :
                                    #print 'found', subnode.attrib, subnode.get("v")
                                    
										# initialize the node address if have not done so
                                        if "address" not in node :
                                            node["address"] = {}
                                            
                                        if addrec == 'street' :
                                            if len(clean_up_street(subnode.get("v"))) > 0 : 
                                                #print subnode.get("v"), clean_up_street(subnode.get("v"))
                                                node["address"][addrec] = clean_up_street(subnode.get("v"))
                                        elif addrec == 'postcode' :
                                            if len(clean_up_postcode(subnode.get("v"))) > 0 :
                                                #print subnode.get("v"), clean_up_postcode(subnode.get("v"))
                                                node["address"][addrec] =clean_up_postcode(subnode.get("v"))
                                        elif addrec == 'state' :
                                            if len(clean_up_state(subnode.get("v"))) > 0 :
                                                #print subnode.get("v"), clean_up_state(subnode.get("v"))
                                                node["address"][addrec] = clean_up_state(subnode.get("v"))
                                        
                                    #print 'found', subnode.get("k")

			# check all the valid sub attributes we want to include and add it to the output
            for sa in SUBATTRIB :
                if subnode.get("k") == sa :
                    node[sa] = subnode.get("v")
                            
                    
        #print subnode.tag, subnode.attrib
            
        #pprint.pprint(node)
        
        return node
    else:
        return None

def process_map(filename):
    
	# define the file output name
    file_out = "{0}.json".format(filename)
    
	# define the connection string to access the local mongodb instance
    client = MongoClient('localhost', 27017)
	# my mongodb database name for this project is mydb
    db = client.mydb

	# open the output file	
    with codecs.open(file_out, "w") as fo:
		# read every element in the xml file
        for _, element in ET.iterparse(filename):
		
			# process, clean and transform the xml element into json format
            json_data = shape_element(element)
            #pprint.pprint(json_data)
            
            if json_data :
			# if json_data exists
				# writes the json data into file using json.dumps command
                fo.write(json.dumps(json_data, indent=2,default=json_util.default)+"\n")
				# insert the formed json string into mongodb database
                obj_id = db.mydb.insert(json_data)
                #pprint.pprint(json_data)
				# display the mongodb object id for the inserted json data
                print obj_id

if __name__ == "__main__":
    process_map(OSMFILE)