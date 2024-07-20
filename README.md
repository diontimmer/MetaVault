# MetaVault

MetaVault is a simple database for storing metadata associated with (media) files. This is a simple wrapper around sqlite3 which mimics the behavior of a Python dictionary. This is designed to provide an easy-to-use interface for managing metadata.
I am fully aware that this is not the most efficient way to work with sqlite3, but it is a simple way to work with sqlite in an object-oriented way.

## Installation

Install using pip:

```bash
pip install metavault
```

## Usage

```python
from metavault import MetaVaultDatabase

# connect to database
database = MetaVaultDatabase('test.vault')

# create dataset (or version of dataset)
database.create_dataset('test')
dataset = database['test']

# add data to dataset
dataset["riddim.mp3"] = {"artist": "Bounty Killer", "title": "Riddim Killa"}
dataset["ambient.mp3"] = {"artist": "Dog The Bounty Hunter", "title": "Trashcore"}

# iterate
for item in dataset:
    print(f"- {item}") # - {'riddim.mp3': {'artist': 'Bounty Killer', 'title': 'Riddim Killa'}}

# acts like a dictionary
print(f"\n{dataset['riddim.mp3']}") # {'artist': 'Bounty Killer', 'title': 'Riddim Killa'}

# remove data
del dataset['riddim.mp3']
print(dataset.keys()) # ['ambient.mp3']

# remove attribute
dataset.remove_attribute('artist')
print(dataset['ambient.mp3']) # {'title': 'Trashcore'}

# add attribute
dataset.add_attribute('artist')
dataset['ambient.mp3']['artist'] = "Dog The Bounty Hunter"
print(dataset['ambient.mp3']) # {'title': 'Trashcore', 'artist': 'Dog The Bounty Hunter'}

database.close()

# or with context manager
with MetaVaultDatabase('test.locker') as database:
    dataset = database['test']
    # export as various formats
    dataset.export('test.csv')
    dataset.export('test.json')
    dataset.export('test.jsonl')
```