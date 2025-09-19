from typing import TypedDict, Annotated, Sequence
from langchain_core.messages import BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from src.agentic_data_pipeline.tools import list_s3_files, describe_table, list_tables

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], lambda x, y: x + y]

def create_agent():
    tools = [list_s3_files, describe_table, list_tables]
    llm = ChatOllama(model="llama3-groq-tool-use", temperature=0)
    
    with open("prompts/system_prompt.txt", "r") as f:
        system_prompt = f.read()

    prompt = ChatPromptTemplate.from_messages(
        [("system", system_prompt), ("placeholder", "{messages}")]
    )
    model_with_tools = llm.bind_tools(tools)
    chain = prompt | model_with_tools

    def call_model(state):
        response = chain.invoke({"messages": state['messages']})
        return {"messages": [response]}

    def should_continue(state):
        if state['messages'][-1].tool_calls:
            return "continue"
        return "end"

    workflow = StateGraph(AgentState)
    workflow.add_node("agent", call_model)
    workflow.add_node("action", ToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges("agent", should_continue, {"continue": "action", "end": END})
    workflow.add_edge("action", "agent")
    
    return workflow.compile()