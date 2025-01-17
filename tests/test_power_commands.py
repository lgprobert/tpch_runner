import unittest
from unittest.mock import patch

from tpch_runner.commands.power_commands import barchart


class TestBarchart(unittest.TestCase):
    def setUp(self):
        self.title = "Test"
        self.labels = ["Q1", "Q2", "Q3"]
        self.data = [1.0, 2.0, 3.0]
        self.fpath = "test_chart.png"

    @patch("matplotlib.pyplot.title")
    def test_barchart_title(self, mock_title):
        # with patch("matplotlib.pyplot") as mock_plt:
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_title.assert_called_once_with(
            f"{self.title} TPC-H Queries Runtime", fontsize=16
        )

    @patch("matplotlib.pyplot.xlabel")
    @patch("matplotlib.pyplot.ylabel")
    def test_barchart_axis_labels(self, mock_ylabel, mock_xlabel):
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_xlabel.assert_called_once_with("Query", fontsize=14)
        mock_ylabel.assert_called_once_with("Runtime (seconds)", fontsize=14)

    @patch("matplotlib.pyplot.bar")
    def test_barchart_bars(self, mock_bar):
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_bar.assert_called_once_with(self.labels, self.data, color="skyblue")

    @patch("matplotlib.pyplot.savefig")
    def test_barchart_savefig(self, mock_savefig):
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_savefig.assert_called_once_with(self.fpath, dpi=300)

    @patch("matplotlib.pyplot.tight_layout")
    def test_barchart_layout(self, mock_tight_layout):
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_tight_layout.assert_called_once()

    def test_barchart_mismatched_labels_data(self):
        mismatched_labels = ["Q1", "Q2"]
        with self.assertRaises(ValueError):
            barchart(self.title, mismatched_labels, self.data, self.fpath)

    @patch("matplotlib.pyplot.bar")
    @patch("matplotlib.pyplot.title")
    @patch("matplotlib.pyplot.xlabel")
    @patch("matplotlib.pyplot.ylabel")
    @patch("matplotlib.pyplot.tight_layout")
    @patch("matplotlib.pyplot.savefig")
    def test_barchart_overall(
        self,
        mock_savefig,
        mock_tight_layout,
        mock_ylabel,
        mock_xlabel,
        mock_title,
        mock_bar,
    ):
        barchart(self.title, self.labels, self.data, self.fpath)
        mock_bar.assert_called_once_with(self.labels, self.data, color="skyblue")
        mock_title.assert_called_once_with(
            f"{self.title} TPC-H Queries Runtime", fontsize=16
        )
        mock_xlabel.assert_called_once_with("Query", fontsize=14)
        mock_ylabel.assert_called_once_with("Runtime (seconds)", fontsize=14)
        mock_tight_layout.assert_called_once()
        mock_savefig.assert_called_once_with(self.fpath, dpi=300)


if __name__ == "__main__":
    unittest.main()
