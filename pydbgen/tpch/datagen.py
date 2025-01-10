from . import timeit
from .injection import data_gen_batch, table_map


class DataGen:
    def __init__(self, sf: int = 1, delimiter: str = "|"):
        self.delimiter = delimiter
        self.scale_factor = sf

    @timeit
    def generate(self, table: str):
        table_code = table_map.get(table)
        data_gen_batch(table_code, self.scale_factor)  # type: ignore

    @timeit
    def gen_all(self):
        for tbl in table_map.keys():
            self.generate(tbl)
