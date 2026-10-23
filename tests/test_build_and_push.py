import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "build_and_push.sh"


class BuildAndPushTest(unittest.TestCase):
    def run_script(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / "docker"
            docker.write_text('''#!/bin/bash
echo "$*" >> "$DOCKER_TEST_CALLS"
if [ "$1 $2" = "context show" ]; then
    echo orbstack
    exit 0
fi
if [ "$1 $2" = "buildx inspect" ]; then
    if [ "$DOCKER_TEST_MODE" = "new-builder" ]; then
        case "$*" in *--*) ;; *) exit 1 ;; esac
    fi
    case "$*" in
        *--bootstrap*)
            if [ "$DOCKER_TEST_MODE" = "builder-down" ]; then
                echo 'Error: cannot connect to old docker socket'
            else
                echo 'Status: running'
            fi
            ;;
    esac
fi
if [ "$1 $2" = "system info" ] && [ "$DOCKER_TEST_MODE" = "engine-down" ]; then
    echo "Cannot connect to the Docker daemon" >&2
    exit 1
fi
if [ "$1" = "login" ] && [ "$DOCKER_TEST_MODE" = "login-failed" ]; then
    exit 1
fi
exit 0
''')
            docker.chmod(0o755)
            sleep = root / "sleep"
            sleep.write_text("#!/bin/bash\nexit 0\n")
            sleep.chmod(0o755)
            (root / "Dockerfile").write_text("FROM scratch\n")
            calls_path = root / "calls"
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                       DOCKER_TEST_CALLS=str(calls_path), DOCKER_TEST_MODE=mode)
            result = subprocess.run(
                ["bash", str(SCRIPT)], cwd=root, env=env, input="y\n",
                capture_output=True, text=True, timeout=10,
            )
            return result, calls_path.read_text().splitlines()

    def test_successful_login_is_not_retried_or_logged_out(self):
        result, calls = self.run_script("success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sum(call.startswith("login ") for call in calls), 1)
        self.assertFalse(any(call.startswith("logout") for call in calls))
        builds = [call for call in calls if call.startswith("buildx build ")]
        self.assertEqual(len(builds), 1)
        self.assertIn("--builder charming-epg-orbstack --pull --no-cache", builds[0])
        self.assertNotIn("--dry-run", builds[0])

    def test_engine_failure_stops_before_login_and_preserves_error(self):
        result, calls = self.run_script("engine-down")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cannot connect to the Docker daemon", result.stderr)
        self.assertFalse(any(call.startswith("login ") for call in calls))
        self.assertFalse(any(call.startswith("buildx build ") for call in calls))

    def test_new_builder_is_bound_to_current_context(self):
        result, calls = self.run_script("new-builder")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "buildx create --name charming-epg-orbstack --driver docker-container --use orbstack",
            calls,
        )

    def test_inspect_success_with_broken_node_stops_build(self):
        result, calls = self.run_script("builder-down")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("构建器未就绪", result.stdout)
        self.assertFalse(any(call.startswith("buildx build ") for call in calls))

    def test_authentication_failure_retries_without_building(self):
        result, calls = self.run_script("login-failed")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sum(call.startswith("login ") for call in calls), 3)
        self.assertFalse(any(call.startswith("buildx build ") for call in calls))
