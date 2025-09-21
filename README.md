
# Agentic Data Pipeline (POC)

This project is a proof‑of‑concept for building **agentic, auditable data pipelines** powered by LLM reasoning. It demonstrates how an LLM can autonomously:

- Ingest CSVs from S3 into DuckDB as `bronze_<name>` tables  
- Clean and promote them to `silver_<name>` tables  
- Profile the data and build joined/aggregated “gold” tables  

---

## 🚀 Getting started

### 1. Environment (uv)
This repo uses [uv](https://github.com/astral-sh/uv) for dependency management. You’ll find both a `pyproject.toml` and a `uv.lock` file in the root.

- **Install dependencies:** run `uv sync`  
- **What it does:** Creates a virtual environment and installs everything pinned in the lockfile 

---

### 1a. Environment variables
The repo includes an **.env.example** file with the required environment variables.  

- Copy it to a new `.env` file in the project root:  

    cp .env.example .env  

- Fill in your own values (e.g., S3 bucket name, API keys).  
- The pipeline will automatically load these variables at runtime.


---

### 2. Running the pipeline
The entrypoint script is:

    python agent_runner.py

When you run this, the agent will:
1. Compare S3 files to DuckDB bronze tables  
2. Create any missing bronze tables  
3. Promote bronze → silver (cleaning NULLs if needed)  
4. Profile silver tables and build derived aggregates  
5. Write logs to the `logs/` folder  

---

### 3. Model configuration
By default, the pipeline is wired to use a model from Google AI Studio.  

- **To change the model:** edit the configuration in `agent_runner.py`  
- That is the only place you need to update if you want to swap in a different LLM (e.g., GPT, Gemini, Ollama)  

---

### 4. Prompts
The orchestration is driven by two prompt files:

- **System prompt**:  
  `src/agentic_data_pipeline/prompts/system_prompt.txt`  
  Defines the overall mission and rules for the agent  

- **User prompt**:  
  This is in the agent.py file.  
  Defines the specific task for this run (e.g., promote bronze → silver, build aggregates)  

You can edit these to change the agent’s behavior.  

---

### 5. Logs
The agent streams all reasoning steps and tool calls into log files.  

Before running, make sure you have a folder in the repo root:

    mkdir logs

This is where run logs will be collected for each pipeline execution.  

For reference, the repo also includes:

- **LOGS_FROM_EXPERIMENT.log** — a sample log from prior experiments, so users can see how the agent reasons, promotes tables, and records its steps.

---

### 6. Reset & Verification

The repo includes a helper script: **reset_duckdb.py**

This script provides two key functions for users:

- **Verification** — checks that DuckDB is reachable and that the expected tables exist  
- **Reset** — clears out DuckDB state so you can start fresh with a clean pipeline run  


    python reset_duckdb.py


## 🔧 Available tools

- **list_s3_files(bucket_name: str)** — returns all files in the given S3 bucket  
- **list_tables_in_duckdb(database_name: str)** — lists all tables currently in DuckDB  
- **load_csv_to_duckdb(bucket: str, key: str, table: str)** — loads the given S3 file into DuckDB as a `bronze_<name>` table  
- **execute_sql(sql: str)** — runs SQL against DuckDB (query, create, update)  
- **get_table_schema(table: str)** — retrieves column names for a given table  

---

## 📊 Example outputs

- **agg_household_type** — distribution of household categories  
- **agg_people_per_region** — population by region  
- **agg_region_faction** — regions grouped by current faction  

---

## 🧪 Experimentation

This POC was designed to test different LLMs in the same orchestration.

- Start with minimal tools (`execute_sql`)  
- Add helpers (`get_table_schema`) only when needed  
- Swap models in `agent_runner.py` to compare reasoning quality  
- Use schema discovery and clear stop conditions to avoid loops and column‑name hallucinations  


