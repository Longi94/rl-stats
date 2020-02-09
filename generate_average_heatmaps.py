import os
import sys
import gzip
import argparse
import pandas as pd
import numpy as np
from map import standard_maps, maps
from functools import partial
from multiprocessing import Pool
from database import Database, ItemsExtracted
from carball.analysis.utils.proto_manager import ProtobufManager
from carball.analysis.utils.pandas_manager import PandasManager

item_map = {
    'ball_freeze': 1,
    'ball_grappling_hook': 2,
    'ball_lasso': 3,
    'batarang': 3,
    'ball_spring': 4,
    'ball_velcro': 5,
    'boost_override': 6,
    'car_spring': 7,
    'gravity_well': 8,
    'strong_hit': 9,
    'swapper': 10,
    'tornado': 11
}

heatmap_range = ((-4500, 4500), (-6500, 6500))
heatmap_bins = (int(9000 / 50), int(13000 / 50))


def process(replay_hash: str, directory: str):
    try:
        with open(os.path.join(directory, f'stats/{replay_hash}.pts'), 'rb') as f:
            proto = ProtobufManager.read_proto_out_from_file(f)

        with gzip.open(os.path.join(directory, f'df/{replay_hash}.gzip'), 'rb') as f:
            df = PandasManager.read_numpy_from_memory(f)
    except Exception as e:
        return

    item_dfs = []

    for i in range(11):
        item_dfs.append(pd.DataFrame(columns=['pos_x', 'pos_y', 'pos_z']))

    for player in proto.players:
        pdf = df[player.name]
        if 'power_up_active' not in pdf.columns:
            continue
        if 'power_up' not in pdf.columns:
            continue
        pdf.drop('time_till_power_up', axis=1, inplace=True, errors='ignore')
        while len(pdf.loc[(pdf['power_up_active'].shift(1) == True) & (pdf['power_up_active'] == False)]) > 0:
            pdf = pdf.loc[(pdf['power_up_active'].shift(1) != True) | (pdf['power_up_active'] != False)]

        pdf = pdf[pdf['power_up_active'] == False]

        if player.is_orange:
            pdf['pos_y'] = -pdf['pos_y']
            pdf['pos_x'] = -pdf['pos_x']

        for item_name in item_map.keys():
            item_dfs[item_map[item_name] - 1] = item_dfs[item_map[item_name] - 1] \
                .append(pdf.loc[pdf['power_up'] == item_name][['pos_x', 'pos_y', 'pos_z']], ignore_index=True)

    h = list(map(lambda x: np.histogram2d(x['pos_x'], x['pos_y'], heatmap_bins, heatmap_range)[0], item_dfs))

    return h


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, required=True)
    parser.add_argument('-p', '--processes', type=int, required=True)
    args = parser.parse_args()

    db = Database(args.directory)

    processed = 0

    heatmaps = [None] * 11

    items = list(
        map(lambda x: x.hash, filter(lambda x: maps[x.map] in standard_maps, db.Session().query(ItemsExtracted))))
    total = len(items)
    with Pool(args.processes) as p:
        for h in p.imap_unordered(partial(process, directory=args.directory), items):
            if h is None:
                continue
            for i in range(11):
                heatmap = h[i]
                if heatmaps[i] is None:
                    heatmaps[i] = heatmap
                else:
                    heatmaps[i] += heatmap.astype(np.uint)

            processed += 1
            sys.stdout.write(f'\r{processed}/{total}')
            sys.stdout.flush()

    for i in range(11):
        np.save(f'rumble/heatmap{i}', heatmaps[i])
