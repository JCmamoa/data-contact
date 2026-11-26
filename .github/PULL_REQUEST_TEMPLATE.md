## Summary

Describe the change and why it is needed.

## Checklist

- [ ] `python3 -m compileall -q .` passes
- [ ] `python3 -m unittest discover -s tests -p "test_*.py" -v` passes
- [ ] `python3 scripts/generate_showcase.py --check` passes
- [ ] Documentation / ADRs updated if contracts, models, or metrics changed
