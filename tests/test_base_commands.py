import logging
import unittest

from click.testing import CliRunner
from rich_click import RichGroup

from tpch_runner import logger
from tpch_runner.commands.base_commands import CONTEXT_SETTINGS, cli


class TestBaseCommands(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_verbose_mode(self):
        result = self.runner.invoke(cli, ["-v", "version"])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(logger.getEffectiveLevel() == logging.DEBUG)
        for handler in logger.handlers:
            self.assertEqual(handler.level, logging.DEBUG)

    def test_non_verbose_mode(self):
        result = self.runner.invoke(cli)
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(logger.getEffectiveLevel() == logging.DEBUG)
        for handler in logger.handlers:
            self.assertNotEqual(handler.level, logging.DEBUG)

    def test_verbose_mode_stream_handler(self):
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(cli, ["-v", "version"])
            self.assertEqual(result.exit_code, 0)
            stream_handlers = [
                h for h in logger.handlers if isinstance(h, logging.StreamHandler)
            ]
            self.assertTrue(stream_handlers, "No StreamHandler found")
            for handler in stream_handlers:
                self.assertEqual(
                    handler.level, logging.DEBUG, "StreamHandler not set to DEBUG level"
                )

    def test_verbose_mode_file_handler(self):
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(cli, ["-v", "version"])
            self.assertEqual(result.exit_code, 0)
            file_handlers = [
                h for h in logger.handlers if isinstance(h, logging.FileHandler)
            ]
            self.assertTrue(file_handlers, "No FileHandler found")
            for handler in file_handlers:
                self.assertEqual(
                    handler.level, logging.DEBUG, "FileHandler not set to DEBUG level"
                )

    def test_verbose_mode_sets_logger_level(self):
        with self.runner.isolated_filesystem():
            with self.assertLogs(logger, level=logging.DEBUG) as cm:
                result = self.runner.invoke(cli, ["-v", "version"])
                self.assertEqual(result.exit_code, 0)
                self.assertEqual(logger.getEffectiveLevel(), logging.DEBUG)
                for handler in logger.handlers:
                    self.assertEqual(handler.level, logging.DEBUG)
                # breakpoint()
                self.assertTrue(
                    any(
                        f"Verbose mode is on for {logger.name}" in msg
                        for msg in cm.output
                    ),
                    f"Expected log message not found in: {cm.output}",
                )

    def test_verbose_flag_passed_to_context(self):
        result = self.runner.invoke(cli, ["-v", "version"])
        self.assertEqual(result.exit_code, 0)
        with self.runner.isolated_filesystem():
            ctx = cli.make_context("cli", ["-v", "version"])
            cli.invoke(ctx)
            self.assertTrue(ctx.obj["verbose"])
        result = self.runner.invoke(cli)
        self.assertEqual(result.exit_code, 0)
        with self.runner.isolated_filesystem():
            ctx = cli.make_context("cli", ["version"])
            cli.invoke(ctx)
            self.assertFalse(ctx.obj["verbose"])

    def test_non_verbose_mode_logging_levels(self):
        initial_logger_level = logger.level
        initial_handler_levels = [handler.level for handler in logger.handlers]

        result = self.runner.invoke(cli, [])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(logger.level, initial_logger_level)
        for handler, initial_level in zip(logger.handlers, initial_handler_levels):
            self.assertEqual(handler.level, initial_level)
        self.assertNotIn("Verbose mode is on", result.output)

    def test_cli_initialization(self):
        result = self.runner.invoke(cli, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Rapids Installer CLI tool", result.output)
        self.assertIn("-v", result.output)
        self.assertIn("Set for verbose output", result.output)
        self.assertIsInstance(cli, RichGroup)
        self.assertEqual(cli.context_settings, CONTEXT_SETTINGS)

    def test_verbose_option_help_text(self):
        result = self.runner.invoke(cli, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("-v", result.output)
        self.assertIn("Set for verbose output", result.output)

    def test_verbose_mode_logs_debug_message(self):
        with self.assertLogs(logger, level="DEBUG") as log_context:
            result = self.runner.invoke(cli, ["-v", "version"])
            self.assertEqual(result.exit_code, 0)
            self.assertIn(f"Verbose mode is on for {logger.name}", log_context.output[0])

    def test_version_command(self):
        result = self.runner.invoke(cli, ["version"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("TPC-H Runner v1.0", result.output)

    def test_version_command_exit_code(self):
        result = self.runner.invoke(cli, ["version"])
        self.assertEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
