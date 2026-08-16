import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import shard_index  # noqa: E402


BOOTSTRAP_SCRIPT = SCRIPTS / "bootstrap_sparse_clone.py"
SPARSE_PATHS = {".github", "scripts", "tests", "state", "SH6"}


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


class BootstrapSparseCloneTests(unittest.TestCase):
    def make_remote_fixture(self, root: Path) -> tuple[str, dict[str, str]]:
        seed = root / "seed"
        remote = root / "remote.git"
        seed.mkdir()
        git(seed, "init", "-q", "-b", "main")
        git(seed, "config", "user.email", "test@example.invalid")
        git(seed, "config", "user.name", "Test")

        files = {
            "README.md": "fixture\n",
            ".gitignore": "*.tmp\n",
            ".github/workflows/ci.yml": "name: ci\n",
            "scripts/placeholder.py": "print('ok')\n",
            "tests/test_placeholder.py": "def test_placeholder():\n    assert True\n",
            "state/manifest.json": "{}\n",
            "SH6/logs_00.sqlite": "placeholder\n",
            "YOTA_Contest/2026/Round_1/S53ZO.log": "START-OF-LOG: 3.0\nEND-OF-LOG:\n",
            "WAE/2025/AA1ZZ.log": "START-OF-LOG: 3.0\nEND-OF-LOG:\n",
            "RECONSTRUCTED_LOGS/2026/REB.log": "START-OF-LOG: 3.0\nEND-OF-LOG:\n",
        }
        for rel, content in files.items():
            path = seed / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="ascii")

        git(seed, "add", ".")
        git(seed, "commit", "-qm", "fixture")
        git(root, "clone", "-q", "--bare", str(seed), str(remote))
        git(remote, "config", "uploadpack.allowFilter", "true")
        return remote.resolve().as_uri(), {
            "contest_log": "YOTA_Contest/2026/Round_1/S53ZO.log",
            "wae_log": "WAE/2025/AA1ZZ.log",
            "reconstructed_log": "RECONSTRUCTED_LOGS/2026/REB.log",
        }

    def run_bootstrap(self, destination: Path, remote: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(BOOTSTRAP_SCRIPT),
                str(destination),
                "--remote",
                remote,
                "--branch",
                "main",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_bootstrap_creates_depth_1_blobless_sparse_clone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            remote, _logs = self.make_remote_fixture(Path(temp))
            destination = Path(temp) / "clone"

            result = self.run_bootstrap(destination, remote)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Verified remote archive paths: 3", result.stdout)
            self.assertTrue((destination / "README.md").is_file())
            self.assertTrue((destination / ".gitignore").is_file())
            for rel in SPARSE_PATHS:
                self.assertTrue((destination / rel).is_dir(), rel)
            self.assertFalse((destination / "YOTA_Contest").exists())
            self.assertFalse((destination / "WAE").exists())
            self.assertFalse((destination / "RECONSTRUCTED_LOGS").exists())
            self.assertEqual(set(git(destination, "sparse-checkout", "list").splitlines()), SPARSE_PATHS)
            self.assertEqual(git(destination, "config", "--get", "remote.origin.partialclonefilter"), "blob:none")
            self.assertEqual(git(destination, "config", "--bool", "remote.origin.promisor"), "true")
            self.assertEqual(git(destination, "rev-parse", "--is-shallow-repository"), "true")
            self.assertEqual(git(destination, "rev-list", "--count", "HEAD"), "1")

    def test_bootstrap_refuses_to_replace_existing_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            remote, _logs = self.make_remote_fixture(Path(temp))
            destination = Path(temp) / "clone"
            destination.mkdir()
            sentinel = destination / "sentinel.txt"
            sentinel.write_text("keep\n", encoding="ascii")

            result = self.run_bootstrap(destination, remote)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("destination already exists", result.stderr)
            self.assertEqual(sentinel.read_text(encoding="ascii"), "keep\n")

    def test_git_tree_log_enumeration_leaves_sparse_logs_unchecked_out(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            remote, logs = self.make_remote_fixture(Path(temp))
            destination = Path(temp) / "clone"

            result = self.run_bootstrap(destination, remote)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Verified remote archive paths: 3", result.stdout)
            for rel in logs.values():
                self.assertFalse((destination / rel).exists(), rel)
                self.assertEqual(git(destination, "ls-files", "-t", "--", rel), f"S {rel}")

            enumerated = sorted(
                path for path in shard_index._iter_git_paths(destination, "HEAD") if Path(path).suffix.lower() == ".log"
            )

            self.assertEqual(enumerated, sorted(logs.values()))
            self.assertFalse((destination / "YOTA_Contest").exists())
            self.assertFalse((destination / "WAE").exists())
            self.assertFalse((destination / "RECONSTRUCTED_LOGS").exists())


if __name__ == "__main__":
    unittest.main()
