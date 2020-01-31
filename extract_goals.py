import os
import sys
import logging
import argparse
from carball.analysis.utils.proto_manager import ProtobufManager
from database import Database, ItemsExtracted, GoalRecord

log = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', type=str, required=True)
    parser.add_argument('-p', '--processes', type=int, default=1)
    args = parser.parse_args()

    db = Database(args.directory)
    total = db.Session().query(ItemsExtracted).count()
    processed = 0

    for replay in db.Session().query(ItemsExtracted):
        try:
            with open(os.path.join(args.directory, f'stats/{replay.hash}.pts'), 'rb') as f:
                proto = ProtobufManager.read_proto_out_from_file(f)

                for goal in proto.game_metadata.goals:
                    player = next(filter(lambda x: x.id.id == goal.player_id.id, proto.players))

                    goal_record = GoalRecord(
                        parent_id=replay.id,
                        player_id=goal.player_id.id,
                        frame=goal.frame_number,
                        pre_item=goal.extra_mode_info.pre_items,
                        is_orange=player.is_orange
                    )

                    if goal.extra_mode_info.scored_with_item:
                        goal_record.item = goal.extra_mode_info.used_item
                    else:
                        goal_record.item = -1

                    db.Session().add(goal_record)

                if processed % 1000 == 0:
                    db.commit()

        except Exception as e:
            log.error(f'Failed to handle {replay.hash}', exc_info=e)
            db.Session.rollback()

        finally:
            processed += 1
            sys.stdout.write(f'\r{processed}/{total}')
            sys.stdout.flush()

    db.commit()
