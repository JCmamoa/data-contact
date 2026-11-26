# Contributing

1. Fork or branch from `main`.
2. Run compilation, unit/E2E tests, and showcase verification before opening a PR:
   ```bash
   python3 -m compileall -q .
   python3 -m unittest discover -s tests -p "test_*.py" -v
   python3 scripts/generate_showcase.py
   ```
3. Document materialization or contract design decisions in `docs/adr/`.
