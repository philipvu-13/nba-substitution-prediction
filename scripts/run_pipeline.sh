#!/usr/bin/env bash
set -e
cd /home/unclephil/nba-substitution-prediction
source .venv/bin/activate

python src/collect/backfill_season.py --season 2025-26
python src/collect/backfill_rotations_season.py --season 2025-26
python src/collect/backfill_boxscore_minutes_season.py --season 2025-26
python src/processing/build_player_stints.py --season 2025-26
python src/processing/report_edwards_rotation_profile.py --ensure-view
python src/notify/send_discord_summary.py
