"""Recorder package for the arb-bot codegen system.

The recorder captures every action of an arb bet flow as a JSONL trace.
Downstream codegen and replay tools consume traces produced here.

See arbitrage_executor/docs/recorder_workflow.md for usage.
"""
from toolkit.recorder.schema import (  # noqa: F401
    FORMAT_VERSION,
    ACTION_KINDS,
    PHASES,
    SELECTOR_STRATEGIES,
    ElementSignature,
    NetworkEvent,
    TraceRecord,
    TraceHeader,
    to_jsonl_line,
    parse_header,
    parse_record,
    load_trace,
)
