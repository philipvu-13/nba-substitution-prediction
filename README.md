## 🎯 Project Objective
Predict player substitution patterns using historical NBA play-by-play data.

## ❓ Core Question
Can we model when a player is likely to be subbed in or out based on game context?

## 🛠 Tech Stack
- Python
- PostgreSQL
- Raspberry Pi 5 (data collection)
- Discord Webhook notifications

## Phase 3 (Pattern Modeling)
- Create the analytics view:
  - `psql -f sql/004_phase3_rotation_profile.sql`
- Query the profile:
  - `SELECT * FROM edwards_rotation_profile_v;`
- Checkpoint sentence:
  - `SELECT 'Based on all his regular season games (' || regular_season_games || '), ' || player_name || '''s average first rest is ' || avg_first_rest_q1_clock_left || ' left in Q1.' AS checkpoint_sentence FROM edwards_rotation_profile_v;`
- Python automation (no local psql required):
  - `python src/processing/report_edwards_rotation_profile.py --ensure-view`
