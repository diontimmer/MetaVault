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
        self.db_path = db_path
        self.manual_commit = manual_commit
        self.conn = sqlite3.connect(db_path, timeout=5000)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    @property
    def datasets(self):
        """
        Return a list of the names of the datasets in the database.
        """
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row["name"] for row in self.cursor.fetchall()]

    def commit(self):
        """
        Commit the current transaction.
        """
        self.conn.commit()

    def _checked_commit(self):
        if not self.manual_commit:
            self.conn.commit()

    def __getitem__(self, table_name):
        if table_name not in self.datasets:
            raise KeyError(f"Table '{table_name}' does not exist.")
        return DatasetWrapper(self, table_name)

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

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


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


class DatasetWrapper:
    def __init__(self, db, table_name):
        self.db = db
        self.conn = db.conn
        self.cursor = db.conn.cursor()
        self.table_name = table_name

    def __getitem__(self, _filename):
        self.cursor.execute(
            f"SELECT * FROM {self.table_name} WHERE _filename=?", (_filename,)
        )
        row = self.cursor.fetchone()
        if row is None:
            raise KeyError(f"No entry found for _filename '{_filename}'")
        data = {
            key: (value if value is not None else None)
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
        values = list(metadata_dict.values())
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
                    key: (row[key] if row[key] is not None else None)
                    for key in row.keys()
                    if key != "_filename"
                }
            }
            for row in rows
        )

    def keys(self):
        self.cursor.execute(f"SELECT _filename FROM {self.table_name}")
        return [row["_filename"] for row in self.cursor.fetchall()]

    def files(self):
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

        # Create a new table with the remaining columns
        remaining_columns = [
            col
            for col in current_columns
            if col != attribute_name and col != "_filename"
        ]
        columns_definition = ", ".join([f"{col} TEXT" for col in remaining_columns])
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS temp_table (_filename TEXT PRIMARY KEY, {columns_definition})"
        )

        # Copy data to the new table
        columns_to_keep = ["_filename"] + remaining_columns
        self.cursor.execute(
            f"INSERT INTO temp_table SELECT {', '.join(columns_to_keep)} FROM {self.table_name}"
        )

        # Drop the old table and rename the new one
        self.cursor.execute(f"DROP TABLE {self.table_name}")
        self.cursor.execute(f"ALTER TABLE temp_table RENAME TO {self.table_name}")
        self.db._checked_commit()

    def items(self):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return {
            row["_filename"]: {
                key: (row[key] if row[key] is not None else None)
                for key in row.keys()
                if key != "_filename"
            }
            for row in rows
        }.items()

    def values(self):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return [
            {
                key: (row[key] if row[key] is not None else None)
                for key in row.keys()
                if key != "_filename"
            }
            for row in rows
        ]

    def all(self):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = self.cursor.fetchall()
        return {
            row["_filename"]: {
                key: (row[key] if row[key] is not None else None)
                for key in row.keys()
                if key != "_filename"
            }
            for row in rows
        }

    def export(self, file_path: str, file_column_name: str = "file_name"):
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

    def _export_jsonl(self, file_path, file_column_name):
        entries = []
        for _filename, metadata in self.items():
            entries.append({file_column_name: _filename, **metadata})
        with jsonlines.open(file_path, "w") as file:
            file.write_all(entries)

    def _export_json(self, file_path):
        with open(file_path, "w") as file:
            json.dump(self.all(), file, indent=2)

    def _export_csv(self, file_path, file_column_name):
        with open(file_path, "w", newline="") as file:
            writer = csv.writer(file)
            keys = [file_column_name] + list(next(iter(self.values())).keys())
            writer.writerow(keys)
            for _filename, metadata in self.items():
                writer.writerow([_filename] + list(metadata.values()))

    def __repr__(self):
        return json.dumps(self.all(), indent=2)
