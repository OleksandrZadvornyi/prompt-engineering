# LLM Code Credibility Evaluator

This repository contains a comprehensive framework for generating, analyzing, and evaluating Python code produced by Large Language Models (LLMs) based on user stories.

The core of this project is a robust **Credibility Score** calculation that moves beyond simple text similarity, evaluating generated code based on its structure, semantic correctness, and actual runtime performance within a secure Docker sandbox.

## 🚀 Key Features

- **Multi-Model Orchestration**  
  Uses LangChain to interface with various LLMs (e.g. GPT-4o, Llama, DeepSeek) via OpenRouter

- **Prompt Engineering Variants**  
  Supports multiple prompting strategies including Zero-Shot, Chain-of-Thought (CoT), and clustered contexts

- **Secure Execution Sandbox**  
  Runs generated code inside an isolated Docker container to measure execution success, runtime performance, and capture exceptions without risking the host system

- **Static & Semantic Analysis**
  - **Syntax**: AST parsing validation  
  - **Style**: `flake8` compliance  
  - **Typing**: `mypy` type checking

- **Structural Metrics**  
  Calculates Cyclomatic Complexity, AST Depth, Function Size, and Import Redundancy

- **Credibility Scoring**  
  Aggregates confidence (logprobs), structure, semantics, and execution metrics into a single 0–100 score

- **User Story Clustering**  
  Uses Word2Vec and Gaussian Mixture Models (GMM) to cluster and analyze input requirements

## 📂 Project Structure

```text
├── config/                 # Configuration files for experiments and paths
├── data/                   # Input user stories and clustered data
├── dockerfile              # Definition for the execution sandbox environment
├── pipelines/              # Core workflow scripts
│   ├── stories-to-code/    # Pipeline: Generating code from user stories
│   └── code-to-stories/    # Pipeline: Reverse engineering stories from code
├── results/                # Raw JSON responses and HTML reports
├── utils/                  # Metric calculation modules
│   ├── compute_credibility.py
│   ├── compute_code_execution_metrics.py
│   ├── compute_code_semantic_metrics.py
│   └── compute_code_structure_metrics.py
└── ...
````

## 🛠️ Installation

### Prerequisites

* Python 3.11+
* Docker Desktop (running)

### Setup

Clone the repository:

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

Install dependencies:

```bash
pip install -r requirements.txt

# Ensure analysis tools are installed
pip install flake8 mypy radon
```

Build the Docker sandbox.
The execution metrics rely on a Docker image named `python-sandbox`:

```bash
docker build -t python-sandbox -f dockerfile .
```

Create environment variables.
Add a `.env` file in the root directory:

```ini
OPENROUTER_API_KEY=your_key_here
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
```

## ⚙️ Usage

The project is organized into pipelines. The primary workflow lives in `pipelines/stories-to-code`.

### 1. Run the LLM Experiment

Generate code based on the user stories defined in `data/`:

```bash
python pipelines/stories-to-code/01_run_prompts.py
```

### 2. Analyze Outputs

Compute structural, semantic and execution metrics for the generated code.
This step spins up Docker containers to safely execute the code:

```bash
python pipelines/stories-to-code/02_analyze_outputs.py
```

### 3. Generate Reports

Create detailed HTML reports visualizing the Credibility Score and metric breakdowns:

```bash
python pipelines/stories-to-code/03_generate_reports.py
python pipelines/stories-to-code/05_generate_final_report.py
```

## 📊 The Credibility Score

The `compute_credibility` function calculates a weighted score (0–100) based on four pillars:

* **Confidence (30%)**
  Based on the LLM’s token log probabilities and perplexity

* **Structure (15%)**
  Code maintainability metrics such as Cyclomatic Complexity, nesting depth and function size

* **Semantic (25%)**
  Adherence to Python standards including syntax validity, Flake8 errors and Mypy typing errors

* **Execution (30%)**
  Runtime behavior: successful execution, execution time and absence of runtime exceptions

## 🧪 Data Analysis (Clustering)

To analyze the diversity of user stories before generation:

```bash
python data/stories_to_clusters.py
```

This script uses Word2Vec embeddings and Gaussian Mixture Models to determine optimal clusters for input requirements.

## 📄 License

**MIT License**

