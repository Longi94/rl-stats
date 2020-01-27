import sys
from database import Database, ItemsExtracted, ReplayRecord

db = Database('D:\\rumble_replays')

processed = 0
total = db.Session().query(ItemsExtracted.id).count()

for item_e in db.Session().query(ItemsExtracted):
    replay: ReplayRecord = db.Session().query(ReplayRecord).get(item_e.hash)
    item_e.match_date = replay.match_date

    processed += 1
    sys.stdout.write(f'\r{processed}/{total}')
    sys.stdout.flush()

    if processed % 10000 == 0:
        db.commit()

db.commit()
db.close()
