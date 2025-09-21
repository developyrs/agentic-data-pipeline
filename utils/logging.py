import logging
import sys
import json
import datetime
from langchain.callbacks.base import BaseCallbackHandler
from langchain_core.messages import AIMessage, ToolMessage


def get_logger(name: str):
    """Configures and returns a logger with both file + console handlers."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.setLevel(logging.DEBUG)  # capture everything, filter by handler
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        fh = logging.FileHandler("logs/agent_run.log", mode="a")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)

        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger


class LoggingCallbackHandler(BaseCallbackHandler):
    """Streams agent intermediate steps into our logger."""

    def __init__(self, logger):
        self.logger = logger

    def on_llm_start(self, serialized, prompts, **kwargs):
        for prompt in prompts:
            self.logger.debug(f"[LLM START] Prompt:\n{prompt}")

    def on_llm_end(self, response, **kwargs):
        text = response.generations[0][0].text.strip()
        if text:
            self.logger.debug(f"[LLM END] Response: {text}")

    def on_tool_start(self, serialized, input_str, **kwargs):
        self.logger.info(f"[TOOL START] {serialized['name']} input={input_str}")

    def on_tool_end(self, output, **kwargs):
        self.logger.info(f"[TOOL END] Output={output}")

    def on_agent_action(self, action, **kwargs):
        self.logger.debug(f"[AGENT ACTION] {action.log}")

    def on_agent_finish(self, finish, **kwargs):
        self.logger.info(f"[AGENT FINISH] {finish.log}")


def log_run_header(log, thread_id: str, goal: str):
    """Log a header at the start of each run."""
    log.info("=" * 60)
    log.info(f"Starting pipeline run | thread_id={thread_id} | goal={goal}")
    log.info("=" * 60)


def log_final_result(log, result: dict):
    """Extract and log the final agent message cleanly."""
    try:
        final_msg = result["messages"][-1].content
    except Exception:
        final_msg = str(result)
    log.info(f"[FINAL RESULT]\n{final_msg}")


def append_audit_record(thread_id: str, result: dict, path="logs/audit.jsonl"):
    """Append structured run metadata to a JSONL audit log."""
    try:
        final_msg = result["messages"][-1].content
    except Exception:
        final_msg = str(result)

    record = {
        "thread_id": thread_id,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "final_answer": final_msg,
        "messages": [m.content for m in result.get("messages", [])],
    }

    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")
