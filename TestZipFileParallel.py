import os, zipfile
from concurrent.futures import ThreadPoolExecutor, wait
from ZipFileParallel import ZipFileParallel
import time
import unittest
from tempfile import TemporaryDirectory

# generate many files in a directory
def generate_all_files(path):
    # create all files
    for i in range(10):
        # generate data
        data = os.urandom(1000000).hex().encode('ascii')
        # create filenames
        filepath = path/f'data-{i:04d}.csv'
        # save data file
        filepath.write_bytes(data)
        # report progress
        # print(f'.saved {filepath}')

# load the file as a string then add it to the zip in a thread safe manner
def add_file(handle, filepath):
    # load the data as bytes
    data = filepath.read_bytes()
    # add data to zip
    handle.writestr(str(filepath.name), data)
    # report progress
    # print(f'.added {filepath}')

from pathlib import Path

class TestZipFileParallel(unittest.TestCase):
    fpath = None
    temp_dir = None

    @classmethod
    def setUpClass(cls) -> None:
        print('creating some test files')
        cls.temp_dir = TemporaryDirectory()
        cls.fpath = Path(cls.temp_dir.name)
        generate_all_files(cls.fpath)
        print(f'saved files under {cls.fpath}')

    @classmethod
    def tearDownClass(cls) -> None:
        print('clearing up temporary files')
        cls.fpath = None
        cls.temp_dir = None

    def test_Naive(self):
        # open the zip file
        b = time.time()
        with zipfile.ZipFile('onebyone.zip', 'w', compression=zipfile.ZIP_BZIP2) as handle:
            # create the thread pool
            [add_file(handle, f) for f in self.fpath.iterdir()]
        print(f'writing files one by one took {time.time()-b}sec')

    def test_Parallel(self):
        b = time.time()
        with ZipFileParallel('testing.zip', 'w', compression=zipfile.ZIP_BZIP2) as handle:
            # create the thread pool
            with ThreadPoolExecutor() as exe:
                fs = [exe.submit(add_file, handle, f) for f in self.fpath.iterdir()]

            wait(fs)
            for future in fs:
                future.result() # make sure we didn't get an exception
        print(f'writing files in parallel took {time.time()-b}sec')

