import yaml


def dict_from_yaml(yaml_file):
    with open(yaml_file, "r") as file:
        return yaml.load(file, Loader=yaml.Loader)