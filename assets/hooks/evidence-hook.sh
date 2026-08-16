#!/usr/bin/env bash
set -euo pipefail

python3 -c '
import json
import os
import socket
import sys


def emit(value):
    print(json.dumps(value, separators=(",", ":")))


def is_claude_tool(tool_name):
    return tool_name[:1].isupper()


def allow_for(tool_name, reason="Evidence check passed."):
    if is_claude_tool(tool_name):
        return {
            "hookSpecificOutput": {
                "permissionDecision": "allow",
                "permissionDecisionReason": reason,
            }
        }
    return {"decision": "allow", "reason": reason}


def fail_open(tool_name, reason):
    print(f"evidence-hook fail-open: {reason}", file=sys.stderr)
    emit(allow_for(tool_name, "Evidence check failed open."))
    raise SystemExit(0)


def rpc(method, params):
    path = os.environ.get("AH_SOCKET")
    if not path:
        raise RuntimeError("AH_SOCKET is not set")
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(path)
        client.sendall((json.dumps(request, separators=(",", ":")) + "\n").encode())
        client.shutdown(socket.SHUT_WR)
        response = b""
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            response += chunk
    finally:
        client.close()
    if not response.strip():
        return {}
    payload = json.loads(response.decode().splitlines()[0])
    if "error" in payload:
        raise RuntimeError(payload["error"].get("message", "RPC error"))
    return payload.get("result", {})


def tool_path(data):
    tool_input = data.get("tool_input") or data.get("input") or {}
    for key in ("file_path", "path", "filename"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def claude_allow():
    return allow_for("Read")


def claude_deny(path):
    return {
        "hookSpecificOutput": {
            "permissionDecision": "deny",
            "permissionDecisionReason": f"Evidence Required: You must read {path!r} before editing it.",
        }
    }


def gemini_allow():
    return allow_for("read_file")


def gemini_deny(path):
    return {
        "decision": "block",
        "reason": f"Evidence Required: You must read {path!r} before editing it.",
        "systemMessage": "Read-First Gate Blocked Action",
    }


try:
    data = json.load(sys.stdin)
except Exception as err:
    fail_open("", f"invalid hook input: {err}")

tool_name = data.get("tool_name") or data.get("name") or ""
path = tool_path(data)
job_id = os.environ.get("AH_JOB_ID")

claude_read = {"Read"}
claude_write = {"Edit", "Write", "MultiEdit"}
gemini_read = {"read_file"}
gemini_write = {"replace", "write_file"}

if not path or not job_id:
    emit(claude_allow() if tool_name[:1].isupper() else gemini_allow())
    raise SystemExit(0)

if tool_name in claude_read or tool_name in gemini_read:
    try:
        rpc(
            "evidence.insert",
            {
                "agent_id": os.environ.get("AH_AGENT_ID", "a1"),
                "job_id": job_id,
                "evidence_type": "read",
                "subject_path": path,
                "payload": data,
            },
        )
    except Exception as err:
        fail_open(tool_name, err)
    emit(claude_allow() if tool_name in claude_read else gemini_allow())
    raise SystemExit(0)

if tool_name in claude_write or tool_name in gemini_write:
    try:
        result = rpc(
            "job.has_evidence",
            {
                "job_id": job_id,
                "evidence_type": "read",
                "subject_path": path,
            },
        )
    except Exception as err:
        fail_open(tool_name, err)
    if result.get("has_evidence"):
        emit(claude_allow() if tool_name in claude_write else gemini_allow())
    else:
        emit(claude_deny(path) if tool_name in claude_write else gemini_deny(path))
    raise SystemExit(0)

emit(claude_allow() if tool_name[:1].isupper() else gemini_allow())
'
