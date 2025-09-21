import uuid
from pathlib import Path
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from src.agentic_data_pipeline.tools import list_s3_files, list_tables_in_duckdb, load_csv_to_duckdb, execute_sql, get_table_schema
from utils.logging import (
    get_logger,
    LoggingCallbackHandler,
    log_run_header,
    log_final_result,
    append_audit_record,
)

def load_system_prompt():
    return Path("prompts/system_prompt.txt").read_text()

def run_pipeline(llm):
    log = get_logger("agent")
    system_prompt = load_system_prompt()

    tools = [list_s3_files, list_tables_in_duckdb, load_csv_to_duckdb, execute_sql, get_table_schema]
    memory = MemorySaver()

    agent = create_react_agent(llm, tools, checkpointer=memory)

    thread_id = str(uuid.uuid4())
    log_run_header(log, thread_id, "List files in demos S3 bucket")

    result = agent.invoke(
        {
            "messages": [
                ("system", system_prompt),
                ("user", "In the 'demos' S3 bucket, ensure each CSV has a matching bronze_<name> table in DuckDB—list files, compare to existing tables, and create any missing bronze tables. For each bronze_<name> table, if silver_<name> does not exist, check for NULLs and promote the data to silver_<name> (cleaning NULLs if needed). If silver_<name> already exists, profile it and then create a few joined/aggregated derived tables across the silver tables. Summarize which bronze and silver tables were created, how NULLs were handled, and which derived tables were built.")
            ]
        },
        config={
            "configurable": {"thread_id": thread_id},
            "callbacks": [LoggingCallbackHandler(log)],
            "recursion_limit": 50
        },
    )

    log_final_result(log, result)

    append_audit_record(thread_id, result)

    return result