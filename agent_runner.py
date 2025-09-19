from langchain_core.messages import HumanMessage
from src.agentic_data_pipeline.agent import create_agent
from utils.logging import get_logger, log_agent_output

if __name__ == "__main__":
    log = get_logger(__name__)
    log.info("🚀 Starting agent...")
    
    agent_app = create_agent()
    inputs = {"messages": [HumanMessage(content="Use your tools to list all files in the bucket named 'demos'.")]}
    
    log.info(f"💬 Querying agent with input: \"{inputs['messages'][0].content}\"")
    
    for output in agent_app.stream(inputs):
        log_agent_output(log, output)
    
    log.info("✅ Agent run complete.")