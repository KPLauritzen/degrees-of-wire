import pandas as pd
from pathlib import Path, PurePosixPath
from kedro.io import AbstractDataSet
import bz2

class BetterCSVDataSet(AbstractDataSet):
    def __init__(self, filepath, load_args=None, save_args=None):
        self._filepath = PurePosixPath(filepath)
        self.load_args = load_args
        self.save_args = save_args

    def _load(self) -> pd.DataFrame:
        if self.load_args is None:
            self.load_args = {}
        return pd.read_csv(self._filepath, **self.load_args)

    def _save(self, df: pd.DataFrame) -> None:
        if self.save_args is None:
            self.save_args = {}
        df.to_csv(str(self._filepath), **self.save_args)

    def _exists(self) -> bool:
        return Path(self._filepath.as_posix()).exists()

    def _describe(self):
        return self.__dict__


class BZ2TextDataSet(AbstractDataSet):
    def __init__(self, filepath, load_args=None, save_args=None):
        self._filepath = PurePosixPath(filepath)
        self.load_args = load_args

    def _load(self) -> pd.DataFrame:
        if self.load_args is None:
            self.load_args = {}
        with bz2.open(self._filepath, **self.load_args) as f:
            return f.readlines()

    def _save(self):
        pass

    def _exists(self) -> bool:
        return Path(self._filepath.as_posix()).exists()

    def _describe(self):
        return self.__dict__