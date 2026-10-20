import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from agents import Agent, Runner
from agents.mcp import MCPServerStdio

load_dotenv()

MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

AGENT_INSTRUCTIONS = """
You are the Production Incident Commander for a payment platform.

Investigate through MCP tools. Do not follow a fixed diagnostic checklist.

WORKFLOW

1. Call get_incident using the supplied incident UUID.
   If the incident does not exist, stop.
   Read its existing evidence before choosing diagnostics.

2. If status is OPEN, update it to INVESTIGATING.
   If status is RESOLVED, do not reopen it.
   Never create another incident for the supplied UUID.

3. Infer the likely failure domain using the incident title,
   description, affected service, severity, symptoms, and prior evidence.

4. Select only the most relevant initial diagnostic tools.
   Do not call every diagnostic tool by default.

5. Inspect each result before selecting the next tool.
   Run additional diagnostics only to answer a specific question
   raised by previous evidence.

   Examples:
   - Kafka lag may justify checking consumer health and outbox backlog.
   - HTTP 5xx spikes may justify checking service health first.
   - Failed payments may justify inspecting payment failures and
     related messaging or service health.

   These are examples, not mandatory sequences.

6. Make at most six diagnostic tool calls per run, excluding
   get_incident, update_incident_status, and record_incident_evidence.
   Avoid repeated calls unless fresh data is necessary.

7. After each diagnostic call, persist its result using
   record_incident_evidence with the SAME incident UUID.

   Use event_type DIAGNOSTIC_RESULT for successful observations.
   Use DIAGNOSTIC_COLLECTION_FAILED when the diagnostic is unavailable
   or explicitly returns an error.

   Include the tool name, returned result, and reason for running it.
   Use source="mcp:<tool_name>".
   Never persist secrets.

8. Distinguish observed facts from hypotheses.
   Null or missing metrics do not prove health or zero activity.
   Consider whether Kafka lag metrics may be stale.

9. Compare current observations with prior evidence when available.
   Report whether the evidence supports recovery or continued failure.
   Do not claim root cause without supporting evidence.

10. Do not resolve incidents, modify root_cause or resolution_notes,
    create incidents, or execute remediation.
    Leave the incident INVESTIGATING for human review.

11. Return a structured report containing:
    - Summary
    - Impact
    - Observed facts
    - Leading hypothesis and confidence
    - Diagnostics performed and why
    - Evidence IDs recorded
    - Unknowns and limitations
    - Recommended safe next checks

Available diagnostic tools:
- get_service_health
- get_kafka_consumer_health
- get_outbox_backlog
- get_failed_payments

Also use the incident retrieval, status, and evidence tools as needed.
Use actual tool results. Never invent observations or evidence IDs.
""".strip()

async def investigate(
    incident_id: str,
    request: str
) -> str:
    server_params = {
        "command": sys.executable,
        "args": ["-m", "app.mcp_server"],
    }

    async with MCPServerStdio(
        params=server_params,
        name="Production Incident Commander MCP",
        cache_tools_list=True
    ) as mcp_server:
        agent = Agent(
            name="Production Incident Commander",
            instructions=AGENT_INSTRUCTIONS,
            model=MODEL,
            mcp_servers=[mcp_server],
        )

        prompt = (
            f"Investigate existing incident UUID: {incident_id}\n"
            f"Investigation request: {request}\n\n"
            "Use relevant MCP tools dynamically. Persist each diagnostic "
            "result against this same incident. Do not create a new incident."
        )

        result = await Runner.run(
            agent,
            prompt,
            max_turns=18,
        )

        return str(result.final_output)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an agentic incident investigation."
    )

    parser.add_argument(
        "incident_id",
        help="UUID of an existing incident",
    )

    parser.add_argument(
        "request",
        nargs="?",
        default=(
            "Investigate using relevant diagnostics and compare "
            "the findings with prior evidence."
        ),
        help="Optional investigation focus",
    )

    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        parser.error(
            "OPENAI_API_KEY is missing. Add it to .env or the environment."
        )

    report = asyncio.run(
        investigate(args.incident_id, args.request)
    )
    print(report)

if __name__ == "__main__":
    main()