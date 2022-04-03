## OpTrack
Options trading tracker
Supported platforms (CSV Import):
- Schwab

## Installation and usage
Install in development mode + dependencies: `pip install -e .`

### Configuration
optrack is using [Hydra](https://hydra.cc), which enables you to override everything in the config via config files
or command line overrides. It is highly recommended that you read the basic Hydra tutorial.

### Initial setup
- checkout the code from GitHub
- Copy conf/sample-config.yaml to conf/config.yaml
- Edit conf/config.yaml to reflect your default choices

Make sure you have MongoDB installed and that the config db.url parameter points to it.
For typical setup, install mongodb locally.

e.g:
#### Show config:
```yaml
$ python optrack/main.py  --cfg job
action: list
db:
  url: mongodb://localhost:27017
input:
  file: schwab-transactions-dump.csv
filter:
  symbol: null
  underlying: null
  range:
    start: null
    end: null
```

#### Import transactions file
```commandline
$ python optrack/main.py action=import
```

### Listing positions
Listing is the default action.
```commandline
$ python optrack/main.py
...
```
Or:
```commandline
$ python optrack/main.py action=list
...
```

### Filtering
See the filter node in the config to know what can be overriden.
Example override (show all positions where the position contains the regex PRU):
```commandline
$ python optrack/main.py filter.symbol=PRU
...
```

- `filter.symbol`: Case insensitive regex on the symbol, (e.g: `'filter.symbol=PRU 06/17/2022'` will match all PRU positions with this expiration date)
- `filter.underlying`: Case insensitive full word match for underlying symbol (e.g. `filter.underlying=PRU`)
- `filter.range.start`: Filter positions start date. Matching positions opened after the start date (`filter.range.start=1/1/2022`). 
- `filter.range.end`: Filter positions end date. Matching positions closed before the end date (`filter.range.end=12/31/2022`). 

## Build automation
Optrack uses `nox` for build/test automation.
- `nox -s test`: Run tests. You can also run tests directly with pytest if optrack is installed
- `nox -s lint`: Lint the code, fail on error.
- `FIX=1 nox -s lint`: Lint the code, attempt to fix errors automatically.

### TODO:
Run tests automatically on commit (GH actions or circleci)
