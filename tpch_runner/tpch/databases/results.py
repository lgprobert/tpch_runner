from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from tpch_runner.config import Config

from .. import ANSWER_DIR, RESULT_DIR


class Result:

    def __init__(
        self,
        scale: str,
        db_type: Optional[str] = None,
        test_time: Optional[str] = None,
        result_dir: Optional[str] = None,
    ):
        self.result_dir: Path
        self.answer_dir: Path = Path(ANSWER_DIR).joinpath(scale)
        self.db_type = db_type
        self.scale = scale

        if result_dir:
            self.result_dir = Path(RESULT_DIR).joinpath(result_dir)
        elif test_time:
            test_folder = RESULT_DIR.joinpath(f"{db_type}_{test_time}")
            if not test_folder.is_dir():
                raise ValueError(f"Result directory {str(test_folder)} not exists.")
            self.result_dir = test_folder
        else:
            self.result_dir = RESULT_DIR

    def compare_single_query(
        self, csv_file: str, answer_file: Optional[str] = None
    ) -> bool:
        """Compare the result file with the answer file and return True if they are
        identical.

        Args:
            csv_file: Name of the result file.
            answer_file: Name of the answer file.
        """
        file1_path = Path(RESULT_DIR).joinpath(csv_file)
        if answer_file:
            answer_file_path = Path(answer_file)
        else:

            _, idx1, _ = file1_path.stem.split("_")
            answer_file_path = (
                Path(ANSWER_DIR).joinpath(self.scale).joinpath(f"{idx1[1:]}.csv")
            )

        if not file1_path.is_file() or not answer_file_path.is_file():
            raise FileNotFoundError(
                "Result file may not exist in {}, files: {}, {}".format(
                    self.result_dir, csv_file, answer_file
                )
            )

        return self._equals(file1_path, answer_file_path)

    def compare_against_answer(self, file1: str) -> bool:
        """Compare the result file with the answer file and return True if they are
        identical.

        Args:
            file1: Name of the result file.
        """
        file1_path = self.result_dir.joinpath(file1)
        answer_file = self.answer_dir.joinpath(file1)

        if not Path(self.answer_dir).exists():
            raise FileNotFoundError(f"Answer folder not exists: {self.answer_dir}")

        if not file1_path.is_file() or not answer_file.is_file():
            raise FileNotFoundError(
                "Result file may not exist in {}, files: {}, {}".format(
                    self.result_dir, file1, answer_file
                )
            )

        return self._equals(file1_path, answer_file)

    def _equals(self, file1: Path, file2: Path) -> bool:
        """Compare two result files and return True if they are identical.

        Args:
            file1: Name of the first result file.
            file2: Name of the answer file.
        """
        df_file1 = pd.read_csv(file1)
        df_file2 = pd.read_csv(file2)
        if not df_file1.columns.equals(df_file2.columns):
            if self.db_type == "mysql":
                df_file2.columns = df_file1.columns
            else:
                df_file1.columns = df_file2.columns

        if len(df_file1) != len(df_file2):
            return False

        numeric_columns = df_file1.select_dtypes(include=[np.number]).columns
        numeric_comparison = np.isclose(
            df_file1[numeric_columns], df_file2[numeric_columns], atol=Config.precision
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
