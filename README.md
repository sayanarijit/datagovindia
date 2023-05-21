# datagovindia

A rewrite attempt of https://github.com/addypy/datagovindia

## Install

```bash
python -m venv .venv

pip install git+https://github.com/sayanarijit/datagovindia
```

## Usage

### Python API

```python
api = DataGovIndia()
api.search(title="indian population")
```

### CLI Tool

```bash
datagovindia --help

# Or

python -m datagovindia --help
```

> **NOTE:** First time initialization takes some time to fetch and build local metadata database.
>
> You can refresh the local database by running `datagovindia refresh` or by calling `api.refresh()`.
