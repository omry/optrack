## OpTrack
Options trading tracker
Supported platforms (CSV Import):
- Schwab

## Installation and usage
Install in development mode + dependencies: `pip install -e .`

## Build automation
Optrack uses `nox` for build/test automation.
- `nox -s test`: Run tests. You can also run tests directly with pytest if optrack is installed
- `nox -s lint`: Lint the code, fail on error.
- `FIX=1 nox -s lint`: Lint the code, attempt to fix errors automatically.

### TODO:
Run tests automatically on commit (GH actions or circleci)
