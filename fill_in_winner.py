import sys
from database import Database, ItemsExtracted
from carball.analysis.utils.proto_manager import ProtobufManager

db = Database('D:\\rumble_replays')

processed = 0
total = db.Session().query(ItemsExtracted).count()

for item_e in db.Session().query(ItemsExtracted):
    with open(f'D:\\rumble_replays\\stats/{item_e.hash}.pts', 'rb') as f:
        proto = ProtobufManager.read_proto_out_from_file(f)

    score = proto.game_metadata.score
    if score.team_0_score != score.team_1_score:
        item_e.orange_winner = score.team_0_score < score.team_1_score
    else:
        item_e.orange_winner = None
    processed += 1
    sys.stdout.write(f'\r{processed}/{total}')
    sys.stdout.flush()

    if processed % 1000 == 0:
        db.commit()

db.commit()
db.close()
