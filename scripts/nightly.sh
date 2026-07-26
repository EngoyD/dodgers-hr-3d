#!/bin/bash
# Nightly: refresh current season; Sundays also revalidate clip urls for all seasons.
set -e
cd /Users/dbrr/Dodgers
if [ "$(date +%u)" = "7" ]; then
  for y in 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 2026; do
    .venv/bin/python pipeline.py --year "$y" --refresh-videos || echo "year $y failed" >&2
  done
else
  .venv/bin/python pipeline.py
fi
git add data/trajectories_*.json
if ! git diff --cached --quiet; then
  git commit -m "data: nightly Statcast refresh $(date +%F)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
  git push origin main
fi
