import os
import re
import sys
import gzip
import json
import logging
import argparse
import numpy as np
from functools import partial
from multiprocessing import Pool
from carball.analysis.utils.pandas_manager import PandasManager
from carball.analysis.utils.proto_manager import ProtobufManager
from database import Database, ReplayRecord, ItemRecord, ItemsExtracted
from map import maps

STEAM_ID_PATTERN = re.compile('^7656119[0-9]+$')
log = logging.getLogger(__name__)


def is_steam_id(player_id: str):
    return STEAM_ID_PATTERN.match(player_id) is not None


def is_kickoff_item(frame_get: int, proto, id: str):
    kickoff_frame = next(frame for frame in map(lambda x: x.start_frame_number, reversed(proto.game_stats.kickoffs)) if
                         frame < frame_get)

    item_get_frames = list(
        map(lambda x: x.frame_number_get, filter(lambda x: x.player_id.id == id, proto.game_stats.rumble_items)))

    # first item get frame after kick off
    next_get_frame = next((frame for frame in item_get_frames if frame > kickoff_frame), -1)

    return frame_get == next_get_frame


def process_replay(replay: ReplayRecord, directory: str):
    try:
        replay['mmrs'] = json.loads(replay['mmrs'])
        replay['ranks'] = json.loads(replay['ranks'])

        rank_mmr = list(zip(replay['mmrs'], replay['ranks']))
        rank_mmr = list(filter(lambda x: x[0] is not None and x[0] > 0, rank_mmr))

        replay_data = {
            'hash': replay['hash'],
            'map': None,
            'avg_mmr': None,
            'avg_rank': None
        }

        if len(rank_mmr) == 0:
            return None, replay_data

        if all(map(lambda x: x[1] == 0, rank_mmr)):
            return None, replay_data

        avg_mmr = np.average(list(map(lambda x: x[0], rank_mmr))).item()
        avg_rank = round(np.average(list(filter(lambda x: x > 0, map(lambda x: x[1], rank_mmr)))).item())

        replay_data['avg_mmr'] = avg_mmr
        replay_data['avg_rank'] = avg_rank

        with gzip.open(os.path.join(directory, f'df/{replay["hash"]}.gzip'), 'rb') as f:
            frames = PandasManager.read_numpy_from_memory(f)

        with open(os.path.join(directory, f'stats/{replay["hash"]}.pts'), 'rb') as f:
            proto = ProtobufManager.read_proto_out_from_file(f)

        replay_data['map'] = maps.inverse[proto.game_metadata.map]

        ids = dict(map(lambda x: (x.id.id, (x.name, x.is_orange)), proto.players))

        events = []

        for event in proto.game_stats.rumble_items:
            df = frames[ids[event.player_id.id][0]]

            item_event = {
                'player_id': event.player_id.id,
                'frame_get': event.frame_number_get,
                'frame_use': event.frame_number_use,
                'item': event.item,
                'use_x': None,
                'use_y': None,
                'use_z': None,
                'wait_time': None,
                'is_kickoff': is_kickoff_item(event.frame_number_get, proto, event.player_id.id),
                'is_orange': ids[event.player_id.id][1] == 1,
            }

            if event.frame_number_use > -1:
                item_event['use_x'] = df.loc[event.frame_number_use]['pos_x'].item()
                item_event['use_y'] = df.loc[event.frame_number_use]['pos_y'].item()
                item_event['use_z'] = df.loc[event.frame_number_use]['pos_z'].item()
                item_event['wait_time'] = frames['game'].loc[event.frame_number_use]['time'].item() - \
                                          frames['game'].loc[event.frame_number_get]['time'].item()

            events.append(item_event)

        return events, replay_data
    except Exception as e:
        log.error(f'Failed to handle {replay["hash"]}', exc_info=e)
        return None, None


def add_to_db(events, db: Database, replay_data):
    try:
        if replay_data is None:
            return

        extracted = ItemsExtracted.create(replay_data)
        db.Session().add(extracted)
        db.commit()

        if events is None:
            return

        for event in events:
            record = ItemRecord.create(event)
            record.parent_id = extracted.id
            record.parent = extracted
            db.add(record)
        db.commit()
    except Exception as e:
        log.error('', exc_info=e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, required=True)
    parser.add_argument('-p', '--processes', type=int, default=1)
    args = parser.parse_args()

    db = Database(args.directory)

    replays = list(map(lambda x: x.as_dict(), db.get_replays()))

    # process_replay(next(filter(lambda x: x['hash'] == 'AF9E2D4211E8C1AF5DB54DB4C37FF21A', replays)), args.directory)

    progress = 0

    if args.processes > 1:
        with Pool(args.processes) as p:
            for events, replay_data in p.imap_unordered(partial(process_replay, directory=args.directory), replays,
                                                        chunksize=10):
                add_to_db(events, db, replay_data)
                progress += 1
                sys.stdout.write(f'\r{progress}/{len(replays)}')
                sys.stdout.flush()

    else:
        for replay in replays:
            events, replay_data = process_replay(replay, args.directory)
            add_to_db(events, db, replay_data)
            progress += 1
            sys.stdout.write(f'\r{progress}/{len(replays)}')
            sys.stdout.flush()
