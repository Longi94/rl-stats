import argparse
import requests
import logging
import os
import gzip
import carball
from database import Database, ReplayRecord

HOST = 'https://calculated.gg'


def carball_parse(hash, output_dir):
    pts_file = os.path.join(output_dir, f'{hash}.pts')
    gzip_file = os.path.join(output_dir, f'{hash}.gzip')

    if os.path.exists(pts_file) and os.path.exists(gzip_file):
        return

    manager = carball.analyze_replay_file(os.path.join(output_dir, f'{hash}.replay'))

    with open(os.path.join(output_dir, f'{hash}.pts'), 'wb') as f:
        manager.write_proto_out_to_file(f)

    with gzip.open(os.path.join(output_dir, f'{hash}.gzip'), 'wb') as f:
        manager.write_pandas_out_to_file(f)


def get_replays_from_response(response, output_dir, db):
    log.info(f"Downloading replays from page {response['page']}")
    for replay in response['data']:
        download_url = HOST + replay['download']
        hash = replay['hash']

        if db.get(hash) is not None:
            continue

        file_path = os.path.join(output_dir, f'{hash}.replay')

        if os.path.exists(file_path):
            log.info(f'{file_path} already exists')
        else:
            log.info(f'Downloading replay {hash}, saving to {file_path}')
            replay_response = requests.get(download_url)

            with open(file_path, 'wb') as f:
                f.write(replay_response.content)

        carball_parse(hash, output_dir)
        os.remove(file_path)
        db.add(ReplayRecord.create(replay))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', '-a', type=str, required=True, metavar='api_key')
    parser.add_argument('--output-dir', '-o', type=str, required=True, metavar='output_dir')
    args = parser.parse_args()

    logging.basicConfig(handlers=[logging.StreamHandler()], format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.DEBUG)

    log = logging.getLogger('downloader')

    log.info('Starting...')

    db = Database(args.output_dir)

    os.makedirs(args.output_dir, exist_ok=True)

    json_response = requests.get(f'{HOST}/api/v1/replays?key={args.api_key}&playlist=28').json()
    get_replays_from_response(json_response, args.output_dir, db)
    next_url = json_response.get('next', None)

    while next_url:
        json_response = requests.get(f'{HOST}{next_url}').json()
        get_replays_from_response(json_response, args.output_dir, db)
        next_url = json_response.get('next', None)
