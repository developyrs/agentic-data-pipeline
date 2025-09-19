import logging
import sys

def get_logger(name: str):
    """Configures and returns a basic logger."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/agent_run.log", mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(name)

# Keep the log formatter function as it is
from langchain_core.messages import AIMessage, ToolMessage

def log_agent_output(log, output: dict):
    # ... (this function remains the same as before)
    for key, value in output.items():
        last_message = value['messages'][-1]
        log_message = f"Node '{key}': "
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            tool_call = last_message.tool_calls[0]
            log_message += f"Tool Call to '{tool_call['name']}' with args {tool_call['args']}"
        elif isinstance(last_message, ToolMessage):
            log_message += f"Tool '{last_message.name}' result: {last_message.content}"
        else:
            log_message += f"Response: \"{last_message.content}\""
        log.info(log_message)