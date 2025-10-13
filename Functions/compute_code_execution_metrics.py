import tempfile
import os
import time
import docker
import textwrap

def compute_code_execution_metrics(code_text, timeout_sec=5):
    """
    Safely executes generated Python code in a Docker container.
    Returns:
      - execution_success (bool)
      - execution_time_sec (float)
      - exception_type (str)
      - exception_message (str)
      - runtime_output (str)
    """
    # Create a temporary file for the code
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as tmp:
        tmp.write(code_text)
        tmp_path = os.path.abspath(tmp.name)  # Use absolute path for Windows compatibility

    start_time = time.time()
    execution_success = False
    exec_time = 0.0
    exception_type = ""
    exception_message = ""
    runtime_output = ""

    try:
        # Connect to Docker (uses environment defaults, works on Windows)
        client = docker.from_env()

        # Run the container with isolation:
        # - Image: python:3.11-slim (lightweight Python 3.11)
        # - Command: Run the mounted script
        # - Volumes: Mount the temp file read-only
        # - Network: Disabled to prevent network access
        # - Memory/CPU limits: Prevent resource abuse
        # - Detach: True to run in background
        # - Remove: True to auto-cleanup after exit
        container = client.containers.run(
            "python:3.11-slim",
            ["python", "/app/code.py"],  # Command to execute the script
            volumes={tmp_path: {"bind": "/app/code.py", "mode": "ro"}},  # Mount Windows path to container path
            network_disabled=True,  # No network access
            mem_limit="100m",  # 100 MB memory limit
            cpu_period=100000,  # CPU quota (with quota below, limits to ~0.5 CPU)
            cpu_quota=50000,
            detach=True,
            remove=False  # Auto-remove container after stop
        )

        # Wait for container to finish or timeout
        try:
            result = container.wait(timeout=timeout_sec)
            exec_time = round(time.time() - start_time, 3)
            execution_success = result["StatusCode"] == 0
            logs = container.logs().decode("utf-8", errors="ignore").strip()
        except Exception as e:  # Handles timeout
            container.kill()  # Force stop if timed out
            exec_time = timeout_sec
            exception_type = "TimeoutError"
            exception_message = f"Execution exceeded {timeout_sec}s limit"
        finally:
            try:
                container.remove(force=True)
            except Exception:
                pass
        
        # Clean up dead containers periodically
        # Just in case a crash prevents removal:
        # docker container prune -f

        if not execution_success:
            # Parse exception from logs if possible
            if "Traceback" in logs:
                exception_lines = logs.splitlines()
                last_line = exception_lines[-1] if exception_lines else ""
                if ":" in last_line:
                    exception_type = last_line.split(":")[0].strip()
                    exception_message = ":".join(last_line.split(":")[1:]).strip()
                else:
                    exception_type = "UnknownError"
                    exception_message = logs
            else:
                exception_type = "RuntimeError"
                exception_message = logs

        # Preview first 10 lines of output, shortened
        stdout_preview = "\n".join(logs.splitlines())
        runtime_output = stdout_preview

    except docker.errors.DockerException as e:
        exec_time = round(time.time() - start_time, 3)
        exception_type = type(e).__name__
        exception_message = str(e)
    finally:
        # Clean up temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    return {
        "execution_success": execution_success,
        "execution_time_sec": exec_time,
        "exception_type": exception_type,
        "exception_message": exception_message,
        "runtime_output": runtime_output
    }