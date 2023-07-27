import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from src.tools import DirectoryManager,DownloadProgressBar


class TestTools(unittest.TestCase):

    def setUp(self):
        self.manager = DirectoryManager()
        self.root_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','data'))
        self.new_directory = os.path.join(self.root_data,"new_directory")
        self.new_directory2 = os.path.join(self.root_data,"new_directory2")

    def tearDown(self):
        # Clean up any created directories after each test (optional)
        if os.path.exists(self.new_directory):
            os.rmdir(self.new_directory)
        if os.path.exists(self.new_directory2):
            os.rmdir(self.new_directory2)

    def test_create_new_directory(self):
        path = self.new_directory
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

    def test_create_existing_directory(self):
        path = self.root_data
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

    def test_check_created_directory(self):
        path = self.new_directory2
        self.assertFalse(os.path.exists(path))  # Ensure the directory doesn't exist initially
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

class TestDownloadProgressBar(unittest.TestCase):

    def test_update_progress_single_chunk(self):
        progress_bar = DownloadProgressBar()
        previous_n = progress_bar.n
        b, bsize, tsize = 1, 1024, None
        progress_bar.update_to(b, bsize, tsize)
        self.assertEqual(progress_bar.n, previous_n + b * bsize)

    def test_update_progress_with_total_size(self):
        progress_bar = DownloadProgressBar()
        previous_n = progress_bar.n
        b, bsize, tsize = 1, 1024, 2048
        progress_bar.update_to(b, bsize, tsize)
        self.assertEqual(progress_bar.n, previous_n + b * bsize)
        self.assertEqual(progress_bar.total, tsize)

    def test_update_progress_partial_download(self):
        progress_bar = DownloadProgressBar()
        previous_n = progress_bar.n
        b, bsize, tsize = 2, 512, 2048
        progress_bar.update_to(b, bsize, tsize)
        self.assertEqual(progress_bar.n, previous_n + (b * bsize) - previous_n)

if __name__ == "__main__":
    unittest.main()