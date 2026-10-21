import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from agents import Agent, Runner
from agents.mcp import MCPServerStdio

from app.incident_instructions import (
    DETECT_INSTRUCTIONS,
    INVESTIGATE_INSTRUCTIONS
)

load_dotenv()

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

MAX_AGENT_TURNS = max(
    4,
    min(int(os.getenv("INCIDENT_AGENT_MAX_TURNS", "24")), 60),
)

async def _run(instructions: str, prompt: str) -> str:
    params = {"command": sys.executable, "args": ["-m", "app.mcp_server"]}
    async with MCPServerStdio(
        params=params,
        name="Production Incident Commander MCP",
        cache_tools_list=True,
    ) as server:
        agent = Agent(
            name="Production Incident Commander",
            instructions=instructions,
            model=MODEL,
            mcp_servers=[server],
        )
        result = await Runner.run(agent, prompt, max_turns=MAX_AGENT_TURNS)
        return str(result.final_output)

async def investigate(incident_id: str, request: str) -> str:
    prompt = (
        f"Investigate existing incident UUID: {incident_id}\n"
        f"Request: {request}\nRead prior evidence, choose diagnostics dynamically, "
        "and persist findings against this same UUID."
    )
    return await _run(INVESTIGATE_INSTRUCTIONS, prompt)

async def detect_and_investigate(request: str) -> str:
    prompt = (
        "Run autonomous detection. Check active incidents, inspect telemetry "
        "over a window that includes the symptom onset, create an incident only "
        "if the detection policy is supported by actual evidence, otherwise "
        "reuse a matching active incident or report NO_ACTIONABLE_INCIDENT. "
        "Investigate selected incident and persist evidence.\n\nRequest: " + request
    )
    return await _run(DETECT_INSTRUCTIONS, prompt)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Autonomous incident detection or existing-incident investigation."
    )
    parser.add_argument("incident_id", nargs="?", help="Existing incident UUID")
    parser.add_argument("request", nargs="?", default=None, help="Request focus")
    parser.add_argument("--mode", choices=("investigate", "detect"), default="investigate")
    parser.add_argument("--request", dest="request_flag", default=None)
    args = parser.parse_args()
    if not os.getenv("OPENAI_API_KEY"):
        parser.error("OPENAI_API_KEY is missing. Add it to .env or environment.")
    request = args.request_flag or args.request or (
        "Detect actionable symptoms and create/reuse then investigate an incident."
        if args.mode == "detect"
        else "Investigate relevant diagnostics and compare prior evidence."
    )
    if args.mode == "detect":
        report = asyncio.run(detect_and_investigate(request))
    else:
        if not args.incident_id:
            parser.error("incident_id is required unless --mode detect is selected.")
        report = asyncio.run(investigate(args.incident_id, request))
    print(report)

if __name__ == "__main__":
    main()
