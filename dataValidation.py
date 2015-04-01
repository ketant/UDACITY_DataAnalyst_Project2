# Name : DataValidation.Py 
# Created By : Tan Kwok Wee
# Purpose : Checking of OpenStreetMap Data - Calinfornia.osm for
# 1. Nbr of Unique Tags
# 2. Nbr of Unique Values for addr:street, addr:state, addr:postcode, addr:phone
# 3. Nbr of values for tag k that are lowercase, lower colon or with problem characters
import pprint
import xml.etree.cElementTree as ET
import re
import pymongo

#global variables
OSMFILE = "C:\\Users\\kw\\Documents\\Personal\\DataAnalyst_2015\\Project_2\\san-francisco.osm"

# regular expression checks
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
state = re.compile(r'^\d{5}$')

problemattrib = ["addr:street","addr:state","addr:postcode","phone"]

# check nbr of unique tages found in the osm file
# this will help me understand what kinds of xml tags are available in this document

def check_nbr_of_unique_tags(element, keys) :
        
	# if the element tag if exists add one
    if element.tag in keys :
        keys[element.tag] += 1
    else :
	# else assign 1 
        keys[element.tag] =1 
    
    return keys

# check number of "k" tag values that are lower or with lower colon or with problem chars

def key_type(element, keys):
    if element.tag == "tag":
        
        #print element.get('k')
		# check xml "k" if there are lowercase values
        if lower.search(element.get('k')) :
            keys['lower'] += 1
		# check xml "k" if there are colon values
        elif lower_colon.search(element.get('k')) :
            keys['lower_colon'] += 1
		# check xml "k" if there are problematic values
        elif problemchars.search(element.get('k')) :
            keys['problemchars'] += 1
        else :
            keys['other'] += 1
        
    return keys

# check the distinct values for defined problematic tags
# this will help me get a sense of the kind of variations that exists for each problmetic attributes
def get_unique_values(element, key) :
    
    if element.tag == "tag":
        
		# check all the attributes in the problem attributes
        for search_attrib in problemattrib : 
		
            if search_attrib == element.get("k") :
                #print 
        
                if search_attrib in key :
                    if element.get("v") in key[search_attrib]:
                        pass
                    else : 
						# if the attribute value not exists in the set, add
                        key[search_attrib].add(element.get("v"))
                    
                else :
				# if attribute does not exists, add and initialize as a empty set and add the attribute value to the set
                    key[search_attrib] = set()
                    key[search_attrib].add(element.get("v"))
    
    return key

def process_map(filename):

	# initialize the variables
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    unique_val ={}
    unique_tag = {}
    
	# parse the xml file and pass each xml element to function to gather the data required
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)
        unique_val = get_unique_values(element, unique_val)
        unique_tag = check_nbr_of_unique_tags(element, unique_tag)

    return keys, unique_val, unique_tag

def test():
    data_cnt_of_problem_k_val, unique_val_problem_tag, unique_tag = process_map(OSMFILE)
    
	# print the result of the findings
    pprint.pprint(data_cnt_of_problem_k_val)
    pprint.pprint(unique_val_problem_tag)
    pprint.pprint(unique_tag)
    
    pass



if __name__ == "__main__":
    test()
