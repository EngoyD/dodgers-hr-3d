#!/bin/bash
# One-time backfill: build each Statcast-era season, publish as it completes.
cd /Users/dbrr/Dodgers
for y in 2025 2024 2023 2022 2021 2020 2019 2018 2017 2016 2015; do
  echo "=== backfill $y ==="
  if .venv/bin/python pipeline.py --year "$y"; then
    git add "data/trajectories_${y}.json"
    if ! git diff --cached --quiet; then
      git commit -m "data: backfill $y season

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
      git push origin main
    fi
  else
    echo "season $y FAILED — continuing" >&2
  fi
done
