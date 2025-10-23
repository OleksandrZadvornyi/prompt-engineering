import ast
import tempfile
import subprocess
import os

def compute_code_semantic_metrics(code_text):
    """
    Analyzes the quality of a Python code string by checking for syntax,
    style, and type errors.

    Returns a dictionary with:
      - syntax_valid (bool): True if the code has valid Python syntax.
      - flake8_error_count (int): Total count of style and lint issues from flake8.
      - mypy_error_count (int): Total count of type-checking errors from mypy.
    """

    # --- 1. Syntax check ---
    try:
        ast.parse(code_text)
        syntax_valid = True
    except SyntaxError:
        return {
            "syntax_valid": False,
            "flake8_error_count": -1,
            "mypy_error_count": -1
        }

    # Create a temporary file to run flake8 and mypy on
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
        tmp.write(code_text)
        tmp_path = tmp.name

    # --- 2. Run flake8 for style & lint issues ---
    try:
        result = subprocess.run(
            ["flake8", tmp_path, "--ignore=E501"],  # ignore long lines
            capture_output=True, text=True, check=False
        )
        flake8_error_count = len([line for line in result.stdout.splitlines() if line.strip()])
    except FileNotFoundError:
        print("Flake8 command not found. Please ensure it's installed.")
        flake8_error_count = -1 
    except Exception as e:
        print(f"An error occurred while running flake8: {e}")
        flake8_error_count = -1

    # --- 3. Run mypy for type checking ---
    try:
        result = subprocess.run(
            ["mypy", "--ignore-missing-imports", "--no-color-output", tmp_path],
            capture_output=True, text=True, check=False
        )
        mypy_error_count = len([line for line in result.stdout.splitlines() if "error:" in line])
    except FileNotFoundError:
        print("Mypy command not found. Please ensure it's installed.")
        mypy_error_count = -1 
    except Exception as e:
        print(f"An error occurred while running mypy: {e}")
        mypy_error_count = -1

    # Cleanup temp file
    try:
        os.remove(tmp_path)
    except OSError as e:
        print(f"Error removing temporary file: {e}")

    return {
        "syntax_valid": syntax_valid,
        "flake8_error_count": flake8_error_count,
        "mypy_error_count": mypy_error_count,
    }