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
                ("user", "In the 'demos' S3 bucket, ensure each CSV has a matching bronze_<name> table in DuckDB—list files, compare to existing tables, and create any missing bronze tables. Then, for each bronze_<name> table, check for NULLs: if found, clean them and write to silver_<name>; if none, copy directly to silver_<name>.")
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