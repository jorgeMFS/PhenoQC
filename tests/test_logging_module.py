import unittest
import tempfile
import os

from src.logging_module import setup_logging, log_activity


class TestLoggingModule(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_setup_logging_creates_file_and_logs(self):
        setup_logging()
        logs_dir = os.path.join(self.temp_dir.name, "logs")
        files = os.listdir(logs_dir)
        self.assertEqual(len(files), 1)
        log_file = os.path.join(logs_dir, files[0])

        log_activity("info message")
        log_activity("warn message", level="warning")

        with open(log_file) as f:
            content = f.read()
        self.assertIn("info message", content)
        self.assertIn("warn message", content)

    def test_custom_log_file_and_append_mode(self):
        log_name = "test.log"
        setup_logging(log_name)
        log_activity("first")
        setup_logging(log_name, mode="a")
        log_activity("second")

        log_path = os.path.join(self.temp_dir.name, "logs", log_name)
        with open(log_path) as f:
            content = f.read()
        self.assertIn("first", content)
        self.assertIn("second", content)


if __name__ == "__main__":
    unittest.main()
