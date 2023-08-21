import yaml
import json
import xmltodict


def dict_from_yaml(yaml_file):
    with open(yaml_file, "r") as file:
        return yaml.load(file, Loader=yaml.Loader)
    

def xml_to_dict(xml_file):
    with open(xml_file, "r") as file:
        # remove special characters from output field names
        return xmltodict.parse(file.read(), attr_prefix='', cdata_key='')

 
# Opening JSON file
def json_to_dict(json_file):
    with open(json_file) as json_file:
        return json.load(json_file)
    

def dict_to_json(dict, json_file):
    # Serializing json
    json_object = json.dumps(dict, indent=4)
    
    # Writing to sample.json
    with open(json_file, "w") as outfile:
        outfile.write(json_object)