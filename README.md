# MetaVault

MetaVault is a simple database for storing metadata associated with (media) files. This is a simple wrapper around sqlite3 which mimics the behavior of a Python dictionary. This is designed to provide an easy-to-use interface for managing metadata.

If you are trying to write alot of data iteratively to the database, make sure to set manual_commit=True in the database initialization and db.commit() at the end of your loop. This will reduce the number of commits and increase the speed of writing data to the database.

## Installation

Install using pip:

```bash
pip install metavault
```

## Documentation

Extended documentation can be found [here](https://diontimmer.github.io/MetaVault/MetaVault/metavault.html).

## Usage

```python
from metavault import MetaVaultDatabase

# connect to database

database = MetaVaultDatabase('test.vault')

# create dataset (or version of dataset)

database.create_dataset('test', attributes=['artist', 'title']) # preload attributes
database['test'] = {} # different way to create dataset with no attributes

# get dataset

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

with MetaVaultDatabase('test.vault') as database:
    dataset = database['test']
    # export as various formats
    dataset.export_data('test.csv')
    dataset.export_data('test.json')
    dataset.export_data('test.jsonl')
    # or import
    dataset.import_data('test.csv')
    dataset.import_data('test.json')
    dataset.import_data('test.jsonl')

# write alot of data with manual commit to improve performance

with MetaVaultDatabase('test.vault', manual_commit=True) as database:
    database.begin_transaction() # optional, makes a 'restore point' for rollback
    try:

        dataset = database['test']
        dataset.replace_in_attribute('artist', 'Bounty Killer', 'Bounty Killer 2024') # operations on entire set
        for data in datas:
            dataset[data['filename']] = data
        database.commit()

    except Exception as e:
        database.rollback() # rollback to last 'restore point' if available
        print(e)

# make subsets

with MetaVaultDatabase('test.vault') as database:
    subset = database['test'].search(artist='Bounty Killer')
    subset_2 = database['test'].get_subset_by_key(['ambient.mp3', 'riddim.mp3'])
    subset_3 = database['test'].get_subset_by_amount(25, start=5, reverse=True)
    subset_4 = database['test'].get_subset_by_random(25)
    subset_4.export_data('subset_4.csv') # export just a subset
    combined = subset + subset_2 + subset_3 + subset_4
    database['test_subset'] = combined

```
