"""Claude AI chat daemon — event-driven via bridge WebSocket.

Connects to the bridge WebSocket, watches for TEXT_MESSAGE_APP packets
containing the trigger word from trusted nodes, calls `claude -p` to
generate a reply, and sends it back via the bridge REST API.

No Anthropic API key required — uses the local Claude Code CLI.
"""
import asyncio
import base64
import json
import logging
import subprocess
from collections import defaultdict, deque

import aiohttp

import core.bridge_config as _bcfg

logger = logging.getLogger(__name__)

BRIDGE_URL = "http://localhost:8001"
WS_URL     = "ws://localhost:8001/events"
CLAUDE_BIN = "/root/.local/bin/claude"


def _to_node_list(val) -> list[str]:
    if not val:
        return []
    if isinstance(val, str):
        return [x.strip().lower() for x in val.split(",") if x.strip()]
    return [str(x).lower() for x in val]


def _is_allowed(sender_hex: str, cfg: dict) -> bool:
    whitelist = _to_node_list(cfg.get("whitelist"))
    my_nodes  = _to_node_list(cfg.get("my_nodes"))
    allowed   = set(whitelist) | set(my_nodes)
    if not allowed:
        return True
    return sender_hex.lower() in allowed


async def _call_claude(message: str, history: deque, system_prompt: str) -> str:
    """Call `claude -p` non-interactively and return the reply."""
    # Build a compact conversation context as plain text
    ctx_lines = []
    for entry in history:
        role = "User" if entry["role"] == "user" else "Claude"
        ctx_lines.append(f"{role}: {entry['content']}")
    ctx_lines.append(f"User: {message}")
    prompt = "\n".join(ctx_lines)

    cmd = [
        CLAUDE_BIN, "-p",
        "--append-system-prompt", system_prompt,
        prompt,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        reply = stdout.decode().strip()
        if not reply and stderr:
            logger.warning("claude -p stderr: %s", stderr.decode().strip())
        return reply
    except asyncio.TimeoutError:
        logger.error("claude -p timed out")
        return ""
    except Exception:
        logger.exception("claude -p failed")
        return ""


async def _send_reply(session: aiohttp.ClientSession, gateway: str, to_hex: str, text: str):
    url = f"{BRIDGE_URL}/{gateway}/messages"
    try:
        async with session.post(url, json={"text": text, "to": to_hex}) as resp:
            if resp.status != 200:
                logger.warning("send_text returned %s", resp.status)
    except Exception:
        logger.exception("Failed to send reply to %s", to_hex)


async def run():
    """Connect to the bridge WebSocket and handle Claude chat events."""
    cfg = _bcfg.load().get("claude_chat", {})
    if not cfg.get("enabled"):
        logger.info("ClaudeDaemon: disabled in config")
        return

    trigger    = (cfg.get("trigger_word") or "@claude").strip().lower()
    max_reply  = int(cfg.get("max_reply_length") or 200)
    max_hist   = int(cfg.get("max_history") or 20)
    system_prompt = cfg.get("system_prompt") or (
        "You are Claude, accessible via Meshtastic radio. "
        "Keep replies concise — this is a low-bandwidth radio link."
    )

    histories: dict[int, deque] = defaultdict(lambda: deque(maxlen=max_hist))

    logger.info("ClaudeDaemon: started (trigger=%r)", trigger)

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                    logger.info("ClaudeDaemon: WebSocket connected")
                    async for raw in ws:
                        if raw.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            event = json.loads(raw.data)
                        except json.JSONDecodeError:
                            continue

                        if event.get("type") != "packet":
                            continue

                        pkt     = event.get("data", {}).get("packet", {})
                        decoded = pkt.get("decoded", {})
                        if decoded.get("portnum") != "TEXT_MESSAGE_APP":
                            continue

                        try:
                            text = base64.b64decode(decoded["payload"]).decode("utf-8", errors="replace").strip()
                        except Exception:
                            continue

                        if not text.lower().startswith(trigger):
                            continue

                        sender_num = pkt.get("from", 0)
                        sender_hex = f"!{sender_num:08x}"
                        gateway    = event.get("device", "")

                        # Re-read config live so changes take effect without restart
                        cfg = _bcfg.load().get("claude_chat", {})
                        if not cfg.get("enabled"):
                            continue
                        if not _is_allowed(sender_hex, cfg):
                            logger.debug("ClaudeDaemon: %s not in whitelist", sender_hex)
                            continue

                        message = text[len(trigger):].strip() or "Hello!"
                        logger.info("ClaudeDaemon: [%s] %r", sender_hex, message)

                        history = histories[sender_num]
                        reply   = await _call_claude(message, history, system_prompt)
                        if not reply:
                            continue

                        if len(reply) > max_reply:
                            reply = reply[:max_reply - 1] + "…"

                        history.append({"role": "user",      "content": message})
                        history.append({"role": "assistant", "content": reply})

                        await _send_reply(session, gateway, sender_hex, reply)
                        logger.info("ClaudeDaemon: replied to %s: %r", sender_hex, reply)

            except aiohttp.ClientConnectorError:
                logger.warning("ClaudeDaemon: bridge not reachable, retrying in 5s")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info("ClaudeDaemon: stopped")
                return
            except Exception:
                logger.exception("ClaudeDaemon: unexpected error, reconnecting in 5s")
                await asyncio.sleep(5)
