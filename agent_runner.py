import os
from langchain_google_genai import ChatGoogleGenerativeAI
from src.agentic_data_pipeline.agent import run_pipeline

if __name__ == "__main__":
    # Instantiate Gemini via LangChain wrapper
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",   # or gemini-2.0-pro, gemini-2.5-flash, etc.
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        temperature=0.7,
    )

    run_pipeline(llm)
