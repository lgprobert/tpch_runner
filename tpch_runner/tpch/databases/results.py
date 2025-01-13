from typing import Optional

import numpy as np
import pandas as pd

from tpch_runner.config import precision

from .. import RESULT_DIR


class Result:

    def __init__(self, db_type: Optional[str] = None, test_time: Optional[str] = None):
        if test_time:
            test_folder = RESULT_DIR.joinpath(f"{db_type}_{test_time}")
            if not test_folder.is_dir():
                raise ValueError(f"Result directory {str(test_folder)} not exists.")
            self.result_dir = test_folder
        else:
            self.result_dir = RESULT_DIR

    def equals(self, file1: str, file2: str) -> bool:
        file1_path = self.result_dir.joinpath(file1)
        file2_path = self.result_dir.joinpath(file2)
        if not file1_path.is_file() or not file2_path.is_file():
            raise FileNotFoundError(
                "Result file may not exist in {}, files: {}, {}".format(
                    self.result_dir, file1, file2
                )
            )

        db1, idx1, _ = file1_path.stem.split("_")
        db2, idx2, _ = file2_path.stem.split("_")
        if idx1 != idx2:
            raise RuntimeError(
                f"Queries are not matched, first is {idx1}, second is {idx2}."
            )

        df_file1 = pd.read_csv(file1_path)
        df_file2 = pd.read_csv(file2_path)
        if not df_file1.columns.equals(df_file2.columns):
            if db1 == "mysql":
                df_file2.columns = df_file1.columns
            else:
                df_file1.columns = df_file2.columns

        numeric_columns = df_file1.select_dtypes(include=[np.number]).columns
        numeric_comparison = np.isclose(
            df_file1[numeric_columns], df_file2[numeric_columns], atol=precision
        )

        non_numeric_columns = df_file1.select_dtypes(exclude=[np.number]).columns
        stripped_df_file1 = df_file1[non_numeric_columns].map(
            lambda x: x.strip() if isinstance(x, str) else x
        )
        stripped_df_file2 = df_file2[non_numeric_columns].map(
            lambda x: x.strip() if isinstance(x, str) else x
        )
        non_numeric_comparison = stripped_df_file1.equals(stripped_df_file2)

        if numeric_comparison.all() and non_numeric_comparison:
            return True
        print("result from file1:")
        print(df_file1)
        print("-" * 60)
        print("result from file2:")
        print(df_file2)
        return False

    def read_result(self, filename: str) -> pd.DataFrame:
        file_path = self.result_dir.joinpath(filename)
        if not file_path.is_file():
            raise FileNotFoundError(f"File {filename} not found in {self.result_dir}.")

        return pd.read_csv(file_path)
