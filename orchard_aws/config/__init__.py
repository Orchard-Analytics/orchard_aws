import yaml
import os
config_path = os.path.dirname(os.path.abspath(__file__))


def get(config):
    with open(config_path + '/%s.yaml' % config) as f:
        return yaml.safe_load(f)

