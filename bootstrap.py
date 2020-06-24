import os
import gzip
import json
import urllib.request

ETL = {
    'gzip': gzip.decompress,
    '': lambda x: x,
}

def bootstrap(data):
    """Download any external data necessary for analyses"""

    if not os.path.exists('./data'):
        os.mkdir('./data')

    for item in data:
        source = item['source_uri']
        target = item['target_uri']
        etl = item['etl']
        if not os.path.exists(target):
            print('downloading {} ({})...'.format(item['resource'], target))
            with urllib.request.urlopen(source, target) as f:
                open(target, 'wb').write(bytearray(ETL[etl](f.read())))

if __name__ == "__main__":
    with open('data.json', 'r') as f:
        data = json.loads(f.read())
        bootstrap(data)