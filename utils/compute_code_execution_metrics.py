import tempfile
import os
import time
import docker

def _parse_exception_from_logs(logs: str) -> tuple[str, str]:
    """Helper to extract exception details from container logs."""
    if "Traceback" in logs:
        lines = logs.strip().splitlines()
        last_line = lines[-1] if lines else ""
        if ":" in last_line:
            parts = last_line.split(":", 1)
            return parts[0].strip(), parts[1].strip()
        return "UnknownError", last_line
    return "RuntimeError", logs

def compute_code_execution_metrics(code_text, timeout_sec=15):
    """
    Safely executes Python code in a Docker container and returns execution metrics.
    """
    metrics = {
        "execution_success": False,
        "execution_time_sec": 0.0,
        "exception_type": "",
        "exception_message": "",
        "runtime_output": ""
    }

    tmp_file = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8")
    tmp_path = os.path.abspath(tmp_file.name)
    tmp_file.write(code_text)
    tmp_file.close()

    container = None
    start_time = time.time()
    try:
        client = docker.from_env()
        try:
            container = client.containers.run(
                "python-sandbox",
                command=["python", "/app/main.py"],
                volumes={tmp_path: {"bind": "/app/main.py", "mode": "ro"}},
                network_disabled=True,
                mem_limit="100m",
                cpu_period=100000,
                cpu_quota=50000,
                detach=True,
                remove=False,  # Use manual removal for robustness
            )

            result = container.wait(timeout=timeout_sec)
            metrics["execution_success"] = result.get("StatusCode") == 0
        
        except Exception: # Catches timeout from container.wait()
            metrics["exception_type"] = "TimeoutError"
            metrics["exception_message"] = f"Execution exceeded {timeout_sec}s limit"
            if container:
                container.kill()

        if container:
            logs = container.logs().decode("utf-8", errors="ignore").strip()
            metrics["runtime_output"] = logs
            
            # Check for known environment errors first
            is_env_error = False
            if "ModuleNotFoundError" in logs:
                is_env_error = True
            elif "RuntimeError: Directory" in logs and "does not exist" in logs:
                is_env_error = True
            elif "ConfigurationError: Unable to open configuration file" in logs:
                is_env_error = True
            elif "psycopg2.OperationalError" in logs and "could not translate host name" in logs:
                is_env_error = True
            elif "OperationalError" in logs and "no such table" in logs:
                is_env_error = True
            
            if is_env_error:
                metrics["exception_type"] = "EnvironmentMismatch"
                _, metrics["exception_message"] = _parse_exception_from_logs(logs)
            
            # If it's a real code error, parse it as before
            elif not metrics["execution_success"] and not metrics["exception_type"]:
                metrics["exception_type"], metrics["exception_message"] = _parse_exception_from_logs(logs)


    except docker.errors.DockerException as e:
        metrics["exception_type"] = type(e).__name__
        metrics["exception_message"] = str(e)
    finally:
        metrics["execution_time_sec"] = round(time.time() - start_time, 3)
        if container:
            try:
                # Ensure container is removed even if logs() fails
                container.remove(force=True)
            except docker.errors.NotFound:
                pass # Already gone, which is fine
        os.remove(tmp_path)

    return metrics