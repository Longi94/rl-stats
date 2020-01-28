import sys
from map import maps
from database import Database, ItemsExtracted
from carball.analysis.utils.proto_manager import ProtobufManager

db = Database('D:\\rumble_replays')

processed = 0
missing_replays = list(db.Session().query(ItemsExtracted).filter(ItemsExtracted.map == None))

for item_e in missing_replays:
    with open(f'D:\\rumble_replays\\stats/{item_e.hash}.pts', 'rb') as f:
        proto = ProtobufManager.read_proto_out_from_file(f)

    item_e.map = maps.inverse[proto.game_metadata.map]

    processed += 1
    sys.stdout.write(f'\r{processed}/{len(missing_replays)}')
    sys.stdout.flush()

    if processed % 1000 == 0:
        db.commit()

db.commit()
db.close()
