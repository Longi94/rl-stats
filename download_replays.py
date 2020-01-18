import argparse
import requests
import logging
import os
import gzip
import carball
from multiprocessing import Pool
from functools import partial
from database import Database, ReplayRecord

HOST = 'https://calculated.gg'

log = logging.getLogger(__name__)


def carball_parse(hash: str, output_dir: str):
    pts_file = os.path.join(output_dir, 'stats', f'{hash}.pts')
    gzip_file = os.path.join(output_dir, 'df', f'{hash}.gzip')

    if os.path.exists(pts_file) and os.path.exists(gzip_file):
        return

    manager = carball.analyze_replay_file(os.path.join(output_dir, 'replays', f'{hash}.replay'))

    with open(pts_file, 'wb') as f:
        manager.write_proto_out_to_file(f)

    with gzip.open(gzip_file, 'wb') as f:
        manager.write_pandas_out_to_file(f)


def process_replay(replay, output_dir: str, existing_hashes):
    download_url = HOST + replay['download']
    hash = replay['hash']

    if hash in existing_hashes is not None:
        return None

    logging.basicConfig(
        handlers=[logging.FileHandler(os.path.join(output_dir, 'logs', f'{hash}.log'), encoding='utf-8')],
        format='%(asctime)s %(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG)

    try:
        file_path = os.path.join(output_dir, 'replays', f'{hash}.replay')

        if os.path.exists(file_path):
            log.info(f'{file_path} already exists')
        else:
            log.info(f'Downloading replay {hash}, saving to {file_path}')
            replay_response = requests.get(download_url)

            with open(file_path, 'wb') as f:
                f.write(replay_response.content)

        carball_parse(hash, output_dir)
        os.remove(file_path)
        return ReplayRecord.create(replay)
    except Exception as e:
        log.error(f'Failed to process {replay}', exc_info=e)
        return None


def get_replays_from_response(response, output_dir: str, db: Database, process_count: int = 1):
    log.info(f"Downloading replays from page {response['page']}")
    existing = db.get_existing_hashes()
    with Pool(processes=process_count) as p:
        for record in p.imap_unordered(partial(process_replay, output_dir=output_dir, existing_hashes=existing),
                                       response['data'], 1):
            if record is not None:
                db.add(record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', '-a', type=str, required=True)
    parser.add_argument('--output-dir', '-o', type=str, required=True)
    parser.add_argument('--processes', '-p', type=int, default=1)
    parser.add_argument('--log', '-l', type=str, required=True)
    args = parser.parse_args()

    logging.basicConfig(handlers=[logging.StreamHandler(), logging.FileHandler(args.log, encoding='utf-8')],
                        format='%(asctime)s %(levelname)s %(threadName)s %(message)s',
                        level=logging.DEBUG)

    log.info('Starting...')

    db = Database(args.output_dir)

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'replays'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'logs'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'stats'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'df'), exist_ok=True)

    json_response = requests.get(f'{HOST}/api/v1/replays?key={args.api_key}&playlist=28').json()
    get_replays_from_response(json_response, args.output_dir, db, args.processes)
    next_url = json_response.get('next', None)

    while next_url:
        json_response = requests.get(f'{HOST}{next_url}').json()
        get_replays_from_response(json_response, args.output_dir, db, args.processes)
        next_url = json_response.get('next', None)

    db.close()
