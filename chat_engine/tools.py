# from dotenv import load_dotenv

# load_dotenv(override=True)
from langchain_core.tools import tool
from langchain_experimental.utilities import PythonREPL
from typing import Annotated
from langchain_openai import ChatOpenAI


# create a python repl tool for importing in the lessons
repl = PythonREPL()


@tool
def python_repl_tool(
    code: Annotated[str, "The python code to execute to generate your chart."],
):
    """Use this to execute python code. You will be used to execute python code
    that generates charts. Only print the chart once.
    This is visible to the user."""
    try:
        result = repl.run(code)
    except BaseException as e:
        return f"Failed to execute. Error: {repr(e)}"
    result_str = f"Successfully executed:\n```python\n{code}\n```\nStdout: {result}"
    return (
        result_str + "\n\nIf you have completed all tasks, respond with FINAL ANSWER."
    )


reasoning_llm = ChatOpenAI(
    model="o3",
    model_kwargs={"response_format": {"type": "json_object"}},
)
