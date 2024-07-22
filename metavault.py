# META VAULT | A simple database for storing metadata associated with (media) files.
# By Dion Timmer

import random
import sqlite3
import json
import csv
import os
import jsonlines


class MetaVaultDatabase:
    """
    MetaVault is a simple database for storing metadata associated with (media) files.
    This is a simple wrapper around sqlite3 which mimics the behavior of a python dictionary.

    Make sure to enable manual_commit and db.commit() at the end if you are trying to write alot of data iteratively to the database,
    this will reduce the number of commits and increase the speed of writing data to the database.

    Parameters:
        db_path (str): The path to the sqlite3 database file.
        manual_commit (bool, optional): If True, the commit method must be called manually to commit changes to the database. Defaults to False.
    """

    def __init__(self, db_path, manual_commit=False):
        super().__setattr__("db_path", db_path)
        super().__setattr__("manual_commit", manual_commit)
        super().__setattr__("conn", sqlite3.connect(db_path, timeout=5000))
        self.conn.row_factory = sqlite3.Row
        super().__setattr__("cursor", self.conn.cursor())

    @property
    def datasets(self):
        """
        Return a list of the names of the datasets in the database.
        """
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row["name"] for row in self.cursor.fetchall()]

    def begin_transaction(self):
        """
        Begin a new transaction 'restore point' for extra risky operations that might need rolling back.

        Raises:
            NoManualCommitException: If manual_commit is not enabled.
        """
        if not self.manual_commit:
            raise NoManualCommitException("Manual commit is not enabled.")
        self.conn.execute("BEGIN TRANSACTION")

    def rollback_transaction(self):
        """
        Roll back the current transaction in case an error occurred.

        Raises:
            NoManualCommitException: If manual_commit is not enabled.
        """
        if not self.manual_commit:
            raise NoManualCommitException("Manual commit is not enabled.")
        self.conn.execute("ROLLBACK TRANSACTION")

    def commit(self):
        """
        Commit the current transaction.
        """
        self.conn.commit()

    def _checked_commit(self):
        if not self.manual_commit:
            self.conn.commit()

    def __setitem__(self, dataset_name, dataset):
        if dataset_name in self.datasets:
            if self[dataset_name]:
                raise KeyError(
                    f"Dataset '{dataset_name}' already exists and is not empty, please remove using remove_dataset or .clear() it first."
                )
            else:
                self.remove_dataset(dataset_name)

        self.create_dataset(
            dataset_name, attributes=list(dataset[list(dataset.keys())[0]].keys())
        )
        self[dataset_name].batch_insert(dataset)
        self._checked_commit()

    def __getitem__(self, table_name):
        if table_name not in self.datasets:
            raise KeyError(f"Dataset '{table_name}' does not exist.")
        return DatasetWrapper(self, table_name)

    def __delitem__(self, dataset_name):
        self.remove_dataset(dataset_name)

    def __contains__(self, dataset_name):
        return dataset_name in self.datasets

    def __getattribute__(self, name: str):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self[name]

    def __setattr__(self, name: str, value):
        if name in list(self.__dict__.keys()):
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name: str):
        del self[name]

    def create_dataset(self, dataset_name: str, attributes: str = None):
        """
        Create a new dataset in the database.

        Parameters:
            dataset_name (str): The name of the dataset.
            attributes (list of str, optional): The names of the attributes to add to the dataset. Defaults to None.
        """
        if attributes:
            columns_definition = ", ".join([f"{col} TEXT" for col in attributes])
        else:
            columns_definition = ""
        query = f"CREATE TABLE IF NOT EXISTS {dataset_name} (_filename TEXT PRIMARY KEY{', ' + columns_definition if columns_definition else ''})"
        self.cursor.execute(query)
        self._checked_commit()
        return DatasetWrapper(self, dataset_name)

    def remove_dataset(self, dataset_name: str):
        """
        Remove a dataset from the database.

        Parameters:
            dataset_name (str): The name of the dataset.

        """
        self.cursor.execute(f"DROP TABLE IF EXISTS {dataset_name}")
        self._checked_commit()

    def get_dataset(self, dataset_name: str):
        """
        Get a dataset from the database.

        Parameters:
            dataset_name (str): The name of the dataset.

        Returns:
            DatasetWrapper: A wrapper around the dataset.
        """
        return DatasetWrapper(self, dataset_name)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __len__(self):
        return len(self.datasets)


class MetadataDict:
    def __init__(self, dataset, _filename, metadata):
        self.dataset = dataset
        self._filename = _filename
        self.metadata = metadata

    def __getitem__(self, key):
        return self.metadata.get(key)

    def __setitem__(self, key, value):
        self.metadata[key] = value
        self.dataset[self._filename] = self.metadata

    def __repr__(self):
        return repr(self.metadata)


class MetadataCollection:
    def __init__(self, collection_dict=None, metadata_dicts=None):
        if collection_dict is None:
            super().__setattr__("collection_dict", {})
            if metadata_dicts is None:
                raise ValueError(
                    "Either collection_dict or metadata_dicts must be provided."
                )
            for metadata_dict in metadata_dicts:
                self.collection_dict[metadata_dict._filename] = metadata_dict.metadata
        else:
            super().__setattr__("collection_dict", collection_dict)

    def as_dict(self):
        return self.collection_dict

    def __iter__(self):
        return iter(self.collection_dict)

    def __len__(self):
        return len(self.collection_dict)

    def __bool__(self):
        return bool(self.collection_dict)

    def __getitem__(self, key):
        return self.collection_dict.get(key)

    def __setitem__(self, key, value):
        self.collection_dict[key] = value

    def __delitem__(self, key):
        del self.collection_dict[key]

    def __contains__(self, key):
        return key in self.collection_dict

    def __setattr__(self, name, value):
        if name in list(self.__dict__.keys()):
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __getattribute__(self, name: str):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self[name]

    def __delattr__(self, name: str):
        del self[name]

    def __repr__(self):
        return repr(self.collection_dict)

    def __bool__(self):
        return len(self.collection_dict) == 0

    def items(self):
        return self.collection_dict.items()

    def keys(self):
        return self.collection_dict.keys()

    def values(self):
        return self.collection_dict.values()

    def export_data(self, file_path: str, file_column_name: str = "file_name"):
        """
        Export the dataset to a file in jsonl, json or csv format.

        Parameters:
            file_path (str): The path to the file to export the dataset to, picking the file type based on the extension.

        Raises:
            ValueError: If the file type is not one of 'jsonl', 'json' or 'csv'.
        """
        file_type = os.path.splitext(file_path)[1][1:]
        if file_type not in ["jsonl", "json", "csv"]:
            raise ValueError("file type must be one of 'jsonl', 'json', 'csv'")

        if file_type == "jsonl":
            self._export_jsonl(file_path, file_column_name)
        elif file_type == "json":
            self._export_json(file_path)
        elif file_type == "csv":
            self._export_csv(file_path, file_column_name)

    def export(self, file_path: str, file_column_name: str = "file_name"):
        self.export_data(file_path, file_column_name)

    def get_subset_by_key(self, keys):
        """
        Get a subset of the metadata collection based on the provided keys.

        Parameters:
            keys (list): A list of keys to include in the subset.

        Returns:
            MetadataCollection: A collection of metadata entries that match the provided keys.
        """
        return MetadataCollection(
            collection_dict={key: self.collection_dict[key] for key in keys},
        )

    def get_subset_by_amount(self, amount: int, start: int = 0, reverse: bool = False):
        """
        Get a subset of the metadata collection based on the provided amount and start index.

        Parameters:
            amount (int): The amount of metadata entries to include in the subset.
            start (int, optional): The start index of the subset. Defaults to 0.
            reverse (bool, optional): If True, items will be grabbed from the end of the collection. Defaults to False.

        Returns:
            MetadataCollection: A collection of metadata entries that match the provided amount and start index.
        """
        keys = (
            list(self.collection_dict.keys())[::-1][start : start + amount][::-1]
            if reverse
            else list(self.collection_dict.keys())[start : start + amount]
        )
        return MetadataCollection(
            collection_dict={key: self.collection_dict[key] for key in keys},
        )

    def get_subset_by_random(self, amount: int):
        """
        Get a random subset of the metadata collection based on the provided amount.

        Parameters:
            amount (int): The amount of metadata entries to include in the subset.

        Returns:
            MetadataCollection: A collection of random metadata entries that match the provided amount.
        """
        keys = random.sample(list(self.collection_dict.keys()), amount)
        return MetadataCollection(
            collection_dict={key: self.collection_dict[key] for key in keys},
        )

    def merge(self, metadata_collection):
        """
        Merge another metadata collection with this one.

        Parameters:
            metadata_collection (MetadataCollection): The metadata collection to merge with this one.
        """
        self.collection_dict.update(metadata_collection.collection_dict)

    def remove_items(self, keys):
        """
        Remove metadata entries from the collection based on the provided keys.

        Parameters:
            keys (list): A list of keys to remove from the collection.
        """
        for key in keys:
            if not key in self.collection_dict:
                print(f"Key '{key}' not found in collection.")
            self.collection_dict.pop(key, None)

    def __add__(self, metadata_collection):
        self.merge(metadata_collection)
        return self

    def __sub__(self, metadata_collection):
        self.remove_items(metadata_collection.keys())
        return self

    def truncate(self, amount: int):
        """
        Truncate the metadata collection to the provided amount.

        Parameters:
            amount (int): The amount of metadata entries to keep in the collection.
        """
        return self.get_subset_by_amount(amount)

    def _export_jsonl(self, file_path, file_column_name):
        entries = []
        for _filename, metadata in self.items():
            entries.append({file_column_name: _filename, **metadata})
        with jsonlines.open(file_path, "w") as file:
            file.write_all(entries)

    def _export_json(self, file_path):
        with open(file_path, "w") as file:
            json.dump(self.collection_dict, file, indent=2)

    def _export_csv(self, file_path, file_column_name):
        with open(file_path, "w", newline="") as file:
            writer = csv.writer(file)
            keys = [file_column_name] + list(next(iter(self.dataset.values())).keys())
            writer.writerow(keys)
            for _filename, metadata in self.items():
                writer.writerow([_filename] + list(metadata.values()))


class DatasetWrapper:
    def __init__(self, db, table_name):
        super().__setattr__("db", db)
        super().__setattr__("conn", db.conn)
        super().__setattr__("cursor", db.conn.cursor())
        super().__setattr__("table_name", table_name)

    def __getitem__(self, _filename):
        self.cursor.execute(
            f"SELECT * FROM {self.table_name} WHERE _filename=?", (_filename,)
        )
        row = self.cursor.fetchone()
        if row is None:
            raise KeyError(f"No entry found for _filename '{_filename}'")
        data = {
            key: self._deserialize(value) if value is not None else None
            for key, value in dict(row).items()
        }
        data.pop("_filename")
        return MetadataDict(self, _filename, data)

    def __setitem__(self, _filename, metadata_dict):
        current_columns = self._get_columns()
        new_columns = set(metadata_dict.keys()) - set(current_columns)

        for col in new_columns:
            self._add_column(col)

        columns = ", ".join(metadata_dict.keys())
        placeholders = ", ".join(["?" for _ in metadata_dict])
        values = [self._serialize(value) for value in metadata_dict.values()]
        values_with_filename = [_filename] + values

        query = (
            f"INSERT INTO {self.table_name} (_filename, {columns}) VALUES (?, {placeholders}) "
            f"ON CONFLICT(_filename) DO UPDATE SET {', '.join([f'{col}=excluded.{col}' for col in metadata_dict])}"
        )

        self.cursor.execute(query, values_with_filename)
        self.db._checked_commit()

    def __delitem__(self, _filename):
        self.cursor.execute(
            f"DELETE FROM {self.table_name} WHERE _filename=?", (_filename,)
        )
        self.db._checked_commit()

    def __getattribute__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self[name]

    def __setattr__(self, name, value):
        if name in list(self.__dict__.keys()):
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        del self[name]

    def get_subset_by_key(self, keys):
        """
        Get a subset of the dataset based on the provided keys.

        Parameters:
            keys (list): A list of keys to include in the subset.

        Returns:
            MetadataCollection: A collection of metadata entries that match the provided keys.
        """
        return self.all().get_subset_by_key(keys)

    def get_subset_by_amount(self, amount: int, start: int = 0, reverse: bool = False):
        """
        Get a subset of the dataset based on the provided amount and start index.

        Parameters:
            amount (int): The amount of metadata entries to include in the subset.
            start (int, optional): The start index of the subset. Defaults to 0.
            reverse (bool, optional): If True, items will be grabbed from the end of the collection. Defaults to False.

        Returns:
            MetadataCollection: A collection of metadata entries that match the provided amount and start index.
        """
        return self.all().get_subset_by_amount(amount, start, reverse)

    def get_subset_by_random(self, amount: int):
        """
        Get a random subset of the dataset based on the provided amount.

        Parameters:
            amount (int): The amount of metadata entries to include in the subset.

        Returns:
            MetadataCollection: A collection of random metadata entries that match the provided amount.
        """
        return self.all().get_subset_by_random(amount)

    def search(self, **criteria):
        """
        Search the dataset based on given criteria.

        The criteria should be provided as keyword arguments, where the key is the column name and the value is a dictionary specifying the type of search.

        Supported criteria:
            - exact: Exact match. Example: name={"exact": "John Doe"}
            - like: Partial match using SQL LIKE. Example: name={"like": "John"}
            - range: Range query for numerical values. Example: age={"range": [25, 30]}
            - exists: Check if the attribute exists and is not null. Example: email={"exists": True}

        Parameters:
            **criteria: Arbitrary keyword arguments representing search criteria.

        Returns:
            MetadataCollection: A collection of metadata entries that match the search criteria.
            This can be exported to a file using the export method.

        Example usage:
            results = dataset.search(
                name={"like": "John"},
                age={"range": [25, 30]},
                email={"exists": True}
            )
        """
        query = f"SELECT * FROM {self.table_name} WHERE "
        conditions = []
        values = []

        for key, value in criteria.items():
            if isinstance(value, dict):
                if "exact" in value:
                    conditions.append(f"{key} = ?")
                    values.append(value["exact"])
                if "like" in value:
                    conditions.append(f"{key} LIKE ?")
                    values.append(f"%{value['like']}%")
                if "range" in value:
                    conditions.append(f"{key} BETWEEN ? AND ?")
                    values.extend(value["range"])
                if "exists" in value and value["exists"]:
                    conditions.append(f"{key} IS NOT NULL")
            else:
                conditions.append(f"{key} = ?")
                values.append(value)

        query += " AND ".join(conditions)
        self.cursor.execute(query, tuple(values))
        rows = self.cursor.fetchall()
        entries = {
            row["_filename"]: {
                key: (self._deserialize(row[key]) if row[key] is not None else None)
                for key in row.keys()
                if key != "_filename"
            }
            for row in rows
        }

        return MetadataCollection(collection_dict=entries)

    def _serialize(self, value):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    def _deserialize(self, value):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    def __contains__(self, _filename):
        self.cursor.execute(
            f"SELECT COUNT(*) FROM {self.table_name} WHERE _filename=?", (_filename,)
        )
        return self.cursor.fetchone()[0] > 0

    def __len__(self):
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        return self.cursor.fetchone()[0]

    def __bool__(self):
        return len(self) > 0

    def __iter__(self):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return iter(
            {
                row["_filename"]: {
                    key: (self._deserialize(row[key]) if row[key] is not None else None)
                    for key in row.keys()
                    if key != "_filename"
                }
            }
            for row in rows
        )

    def __add__(self, other):
        if not isinstance(other, DatasetWrapper):
            raise TypeError(
                "unsupported operand type(s) for +: 'DatasetWrapper' and '{type(other)}'"
            )
        current_meta = self.all()
        current_meta.merge(other.all())
        self.batch_insert(current_meta)
        return self

    def __sub__(self, other):
        if not isinstance(other, DatasetWrapper):
            raise TypeError(
                "unsupported operand type(s) for -: 'DatasetWrapper' and '{type(other)}'"
            )
        current_meta = self.all()
        current_meta.remove_items(other.keys())
        self.clear()
        self.batch_insert(current_meta)
        return self

    def keys(self):
        """
        Returns a list of all the filenames in the dataset.
        """
        self.cursor.execute(f"SELECT _filename FROM {self.table_name}")
        return [row["_filename"] for row in self.cursor.fetchall()]

    def files(self):
        """
        Returns a list of all the filenames in the dataset.
        """
        return self.keys()

    def _get_columns(self):
        self.cursor.execute(f"PRAGMA table_info({self.table_name})")
        return [info["name"] for info in self.cursor.fetchall()]

    def _add_column(self, column_name):
        self.cursor.execute(
            f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} TEXT"
        )
        self.db._checked_commit()

    def add_attribute(self, attribute_name: str):
        """
        Add an attribute to the dataset.

        Parameters:
            attribute_name (str): The name of the attribute to add.
        """
        current_columns = self._get_columns()
        if attribute_name in current_columns:
            print(f"Attribute '{attribute_name}' already exists.")
            return

        self._add_column(attribute_name)

    def remove_attribute(self, attribute_name: str):
        """
        Remove an attribute from the dataset.
        Warning: This operation is not reversible.

        Parameters:
            attribute_name (str): The name of the attribute to remove.

        Raises:
            KeyError: If the attribute does not exist.
        """

        current_columns = self._get_columns()
        if attribute_name not in current_columns:
            raise KeyError(f"Attribute '{attribute_name}' does not exist.")

        remaining_columns = [
            col
            for col in current_columns
            if col != attribute_name and col != "_filename"
        ]
        columns_definition = ", ".join([f"{col} TEXT" for col in remaining_columns])
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS temp_table (_filename TEXT PRIMARY KEY, {columns_definition})"
        )

        columns_to_keep = ["_filename"] + remaining_columns
        self.cursor.execute(
            f"INSERT INTO temp_table SELECT {', '.join(columns_to_keep)} FROM {self.table_name}"
        )

        self.cursor.execute(f"DROP TABLE {self.table_name}")
        self.cursor.execute(f"ALTER TABLE temp_table RENAME TO {self.table_name}")
        self.db._checked_commit()

    def replace_in_attribute(self, attribute_name: str, old_value: str, new_value: str):
        """
        For all entries in the dataset, replace a value in a specific attribute.

        Parameters:
            attribute_name (str): The name of the attribute to replace the value in.
            old_value (str): The value to replace.
            new_value (str): The new value.

        Raises:
            KeyError: If the attribute does not exist.
        """
        current_columns = self._get_columns()

        if attribute_name not in current_columns:
            raise KeyError(f"Attribute '{attribute_name}' does not exist.")

        self.cursor.execute(f"SELECT rowid, {attribute_name} FROM {self.table_name}")
        rows = self.cursor.fetchall()

        for row in rows:
            rowid, value = row
            if isinstance(value, str):
                new_value_str = value.replace(old_value, new_value)
                self.cursor.execute(
                    f"UPDATE {self.table_name} SET {attribute_name} = ? WHERE rowid = ?",
                    (new_value_str, rowid),
                )

        self.db._checked_commit()

    def batch_insert(self, dataset_entries: dict):
        """
        Insert multiple entries into the dataset.

        Parameters:
            dataset_entries (dict|MetadataCollection): A dictionary where the keys are the filenames and the values are dictionaries of the metadata to insert.

        """
        if not list(dataset_entries.keys()):
            return

        dataset_entries_serialized = {
            filename: {
                key: self._serialize(value) if value is not None else None
                for key, value in metadata.items()
            }
            for filename, metadata in dataset_entries.items()
        }

        # Extract keys and ensure we have the same structure for all entries
        first_key = next(iter(dataset_entries))
        metadata_keys = dataset_entries[first_key].keys()

        columns = "_filename, " + ", ".join(metadata_keys)
        placeholders = ", ".join(["?"] * (len(metadata_keys) + 1))

        query = f"INSERT OR REPLACE INTO {self.table_name} ({columns}) VALUES ({placeholders})"

        # Convert dataset_entries to list of tuples
        values = [
            (filename, *metadata.values())
            for filename, metadata in dataset_entries_serialized.items()
        ]

        self.cursor.executemany(query, values)
        self.db._checked_commit()

    def items(self):
        """
        Returns a collection of metadata entries in the dataset.
        Useful for iterating over the dataset by filename and metadata.
        """
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return MetadataCollection(
            collection_dict={
                row["_filename"]: {
                    key: (self._deserialize(row[key]) if row[key] is not None else None)
                    for key in row.keys()
                    if key != "_filename"
                }
                for row in rows
            },
        ).items()

    def values(self):
        """
        Returns a collection of metadata entries in the dataset without the filenames.
        """
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return [
            {
                key: (self._deserialize(row[key]) if row[key] is not None else None)
                for key in row.keys()
                if key != "_filename"
            }
            for row in rows
        ]

    def all(self):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return MetadataCollection(
            collection_dict={
                row["_filename"]: {
                    key: (self._deserialize(row[key]) if row[key] is not None else None)
                    for key in row.keys()
                    if key != "_filename"
                }
                for row in rows
            },
        )

    def clear(self):
        self.cursor.execute(f"DELETE FROM {self.table_name}")
        self.db._checked_commit()

    def export_data(self, file_path: str, file_column_name: str = "file_name"):
        """
        Export the dataset to a file in jsonl, json or csv format.

        Parameters:
            file_path (str): The path to the file to export the dataset to, picking the file type based on the extension.

        Raises:
            ValueError: If the file type is not one of 'jsonl', 'json' or 'csv'.
        """
        self.all().export_data(file_path, file_column_name)

    def export(self, file_path: str, file_column_name: str = "file_name"):
        """
        Export the dataset to a file in jsonl, json or csv format.

        Parameters:
            file_path (str): The path to the file to export the dataset to, picking the file type based on the extension.

        Raises:
            ValueError: If the file type is not one of 'jsonl', 'json' or 'csv'.
        """
        self.export_data(file_path, file_column_name)

    def import_data(
        self, file_path: str, file_column_name: str = "file_name", append: bool = True
    ):
        """
        Import data from a file in jsonl, json or csv format.

        Parameters:
            file_path (str): The path to the file to import the data from.
            file_column_name (str): The name of the column in the file that contains the filenames.
        """
        if not append:
            self.clear()

        file_type = os.path.splitext(file_path)[1][1:]
        if file_type not in ["jsonl", "json", "csv"]:
            raise ValueError("file type must be one of 'jsonl', 'json', 'csv'")

        if file_type == "jsonl":
            self._import_jsonl(file_path, file_column_name)
        elif file_type == "json":
            self._import_json(file_path)
        elif file_type == "csv":
            self._import_csv(file_path, file_column_name)

        self.db._checked_commit()

    def _import_jsonl(self, file_path, file_column_name):
        with jsonlines.open(file_path) as file:
            for line in file:
                self[line[file_column_name]] = line

    def _import_json(self, file_path):
        with open(file_path) as file:
            data = json.load(file)
            for key, value in data.items():
                self[key] = value

    def _import_csv(self, file_path, file_column_name):
        with open(file_path) as file:
            reader = csv.DictReader(file)
            for row in reader:
                self[row[file_column_name]] = row

    def __repr__(self):
        return json.dumps(self.all().as_dict(), indent=2)


def NoManualCommitException(Exception):
    """
    Exception raised when trying to use manual commit operations without enabling it.
    """
