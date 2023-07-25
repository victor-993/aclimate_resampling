import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from src.tools import DirectoryManager

class TestTools(unittest.TestCase):

    def setUp(self):
        self.manager = DirectoryManager()

    def tearDown(self):
        # Clean up any created directories after each test (optional)
        if os.path.exists("/path/to/new/directory"):
            os.rmdir("/path/to/new/directory")
        if os.path.exists("/path/to/new/directory2"):
            os.rmdir("/path/to/new/directory2")

    def test_create_new_directory(self):
        path = "/path/to/new/directory"
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

    def test_create_existing_directory(self):
        path = "/path/to/existing/directory"
        os.mkdir(path)  # Create the directory before the test
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

    def test_check_created_directory(self):
        path = "/path/to/new/directory2"
        self.assertFalse(os.path.exists(path))  # Ensure the directory doesn't exist initially
        self.manager.mkdir(path)
        self.assertTrue(os.path.exists(path))

    def test_invalid_path(self):
        with self.assertRaises(ValueError):
            self.manager.mkdir("")  # Empty string as path

if __name__ == "__main__":
    unittest.main()