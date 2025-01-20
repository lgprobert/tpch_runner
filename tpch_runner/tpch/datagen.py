from . import timeit
from .injection import TABLE_MAP, data_gen_batch


class DataGen:
    def __init__(self, sf: int = 1, delimiter: str = "|"):
        self.delimiter = delimiter
        self.scale_factor = sf

    @timeit
    def generate(self, table: str):
        table_code = TABLE_MAP.get(table)
        data_gen_batch(table_code, self.scale_factor)  # type: ignore

    @timeit
    def gen_all(self):
        for tbl in TABLE_MAP.keys():
            self.generate(tbl)
