import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class UpdateCommandTests(unittest.TestCase):
    def test_default_broken_ssl_selects_working_installed_python(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / "repo"
            scripts = repo / "scripts"
            scripts.mkdir(parents=True)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            shutil.copy2(ROOT / "scripts/update_last_year_and_push.sh", scripts)
            (scripts / "archive_updater.py").write_text(
                "import sys\nprint('AUDIT_ARGUMENTS', sys.argv[1:])\n"
            )
            binaries = Path(temp) / "bin"
            binaries.mkdir()
            for name, source in {
                "python3": "#!/bin/sh\nexit 1\n",
                "git": f'#!/bin/sh\nif [ "$1" = "pull" ]; then exit 0; fi\nexec "{shutil.which("git")}" "$@"\n',
            }.items():
                path = binaries / name
                path.write_text(source)
                path.chmod(0o755)
            (binaries / "python3.13").symlink_to(sys.executable)
            env = os.environ.copy()
            env.pop("PYTHON_BIN", None)
            env["PATH"] = str(binaries) + os.pathsep + "/usr/bin:/bin"
            result = subprocess.run(
                ["bash", str(scripts / "update_last_year_and_push.sh"), "--workers", "2"],
                cwd=temp, env=env, text=True, capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("using python3.13", result.stderr)
            self.assertIn("'--contests', 'all', '--last', '1', '--publish', '--workers', '2'", result.stdout)

    def test_explicit_broken_interpreter_fails_without_overriding_it(self):
        with tempfile.TemporaryDirectory() as temp:
            executable = Path(temp) / "broken-python"
            executable.write_text("#!/bin/sh\nexit 1\n")
            executable.chmod(0o755)
            env = os.environ.copy()
            env["PYTHON_BIN"] = str(executable)
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/update_last_year_and_push.sh")],
                env=env, text=True, capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("using python", result.stderr)


if __name__ == "__main__":
    unittest.main()
