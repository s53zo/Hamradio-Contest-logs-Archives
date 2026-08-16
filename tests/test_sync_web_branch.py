import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-web-branch.sh"


class SyncWebBranchTests(unittest.TestCase):
    def git(self, repo: Path, *args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def test_builds_reduced_branch_without_copying_archive_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            remote = temp / "remote.git"
            repo = temp / "repo"
            self.git(temp, "init", "--bare", str(remote))
            self.git(temp, "init", "-b", "main", str(repo))
            self.git(repo, "config", "user.name", "Test User")
            self.git(repo, "config", "user.email", "test@example.com")
            self.git(repo, "remote", "add", "origin", str(remote))

            (repo / "SH6").mkdir()
            (repo / "SH6" / "logs_00.sqlite").write_bytes(b"shard")
            (repo / "Contest" / "2026").mkdir(parents=True)
            (repo / "Contest" / "2026" / "CALL.log").write_text("log\n")
            (repo / "README.md").write_text("readme\n")
            (repo / "UPDATER.md").write_text("updater\n")
            self.git(repo, "add", ".")
            self.git(repo, "commit", "-m", "source")
            self.git(repo, "push", "-u", "origin", "main")

            subprocess.run(
                ["bash", str(SYNC_SCRIPT), "--source-ref", "HEAD", "--push"],
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
            )

            published_paths = self.git(repo, "ls-tree", "-r", "--name-only", "Web").splitlines()
            self.assertEqual(
                published_paths,
                ["README.md", "SH6/logs_00.sqlite", "UPDATER.md"],
            )
            first_commit = self.git(repo, "rev-parse", "Web")

            subprocess.run(
                ["bash", str(SYNC_SCRIPT), "--source-ref", "main", "--push"],
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(self.git(repo, "rev-parse", "Web"), first_commit)


if __name__ == "__main__":
    unittest.main()
