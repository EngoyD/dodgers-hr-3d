#!/bin/bash
# Nightly: refresh Statcast data, push to GitHub so the public site updates.
set -e
cd /Users/dbrr/Dodgers
.venv/bin/python pipeline.py
git add data/trajectories_*.json
if ! git diff --cached --quiet; then
  git commit -m "data: nightly Statcast refresh $(date +%F)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
  git push origin main
fi
