"""Claude AI assistant on the Meshtastic mesh.

Subscribes to the bridge event stream. When an incoming TEXT_MESSAGE_APP packet
contains the configured trigger word, calls the Claude API and sends the reply
back via the bridge. Fully event-driven — no polling.

Config (bridge_config.yaml under `claude_chat:`):
  enabled:          bool   — master switch
  trigger_word:     str    — e.g. "@claude" (case-insensitive, stripped from message)
  api_key:          str    — Anthropic API key
  model:            str    — e.g. "claude-haiku-4-5-20251001"
  system_prompt:    str    — injected as system message
  max_history:      int    — messages per sender kept in context
  max_reply_length: int    — truncate reply to this many characters
  whitelist:        list   — !hex node IDs allowed; empty = my_nodes only
  my_nodes:         list   — own node IDs (always allowed)
"""
import asyncio
import logging
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from meshtastic import portnums_pb2

import core.bridge_config as _bcfg

if TYPE_CHECKING:
    from core.bridge import MeshBridge

logger = logging.getLogger(__name__)


class ClaudeChat:
    """Attaches to a MeshBridge and handles @claude mentions in real time."""

    def __init__(self, bridge: "MeshBridge"):
        self._bridge = bridge
        self._histories: dict[int, deque] = defaultdict(lambda: deque())
        self._task: asyncio.Task | None = None
        self._queue: asyncio.Queue | None = None

    def start(self):
        cfg = _bcfg.load().get("claude_chat", {})
        if not cfg.get("enabled"):
            return
        if not cfg.get("api_key"):
            logger.warning("claude_chat enabled but api_key is not set — skipping")
            return
        self._queue = self._bridge.state.subscribe()
        self._task = asyncio.create_task(self._run(), name="claude-chat")
        logger.info("ClaudeChat started (trigger=%r, model=%s)", cfg.get("trigger_word"), cfg.get("model"))

    def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None
        if self._queue is not None:
            self._bridge.state.unsubscribe(self._queue)
            self._queue = None

    def reload(self):
        """Reload config without restarting the service."""
        self.stop()
        self.start()

    async def _run(self):
        assert self._queue is not None
        while True:
            try:
                event = await self._queue.get()
                asyncio.create_task(self._handle(event))
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("ClaudeChat event loop error")

    async def _handle(self, event: dict):
        if event.get("type") != "packet":
            return

        pkt = event.get("data", {}).get("packet", {})
        decoded = pkt.get("decoded", {})
        if decoded.get("portnum") != "TEXT_MESSAGE_APP":
            return

        import base64
        try:
            text = base64.b64decode(decoded["payload"]).decode("utf-8", errors="replace").strip()
        except Exception:
            return

        cfg = _bcfg.load().get("claude_chat", {})
        if not cfg.get("enabled"):
            return

        trigger = (cfg.get("trigger_word") or "@claude").strip().lower()
        if not text.lower().startswith(trigger):
            return

        sender_num: int = pkt.get("from", 0)
        sender_hex = f"!{sender_num:08x}"

        if not self._is_allowed(sender_hex, cfg):
            logger.debug("ClaudeChat: sender %s not in whitelist — ignoring", sender_hex)
            return

        message = text[len(trigger):].strip()
        if not message:
            message = "Hello!"

        logger.info("ClaudeChat: message from %s: %r", sender_hex, message)

        reply = await self._call_claude(sender_num, message, cfg)
        if reply:
            # Truncate to fit LoRa practical limits
            max_len = int(cfg.get("max_reply_length") or 200)
            if len(reply) > max_len:
                reply = reply[:max_len - 1] + "…"
            try:
                await self._bridge.send_text(reply, to=sender_num)
                logger.info("ClaudeChat: replied to %s: %r", sender_hex, reply)
            except Exception:
                logger.exception("ClaudeChat: failed to send reply to %s", sender_hex)

    @staticmethod
    def _to_node_list(val) -> list[str]:
        if not val:
            return []
        if isinstance(val, str):
            return [x.strip().lower() for x in val.split(",") if x.strip()]
        return [str(x).lower() for x in val]

    def _is_allowed(self, sender_hex: str, cfg: dict) -> bool:
        whitelist = self._to_node_list(cfg.get("whitelist"))
        my_nodes = self._to_node_list(cfg.get("my_nodes"))
        allowed = set(whitelist) | set(my_nodes)
        if not allowed:
            return True  # no restrictions configured — allow all
        return sender_hex.lower() in allowed

    async def _call_claude(self, sender_num: int, message: str, cfg: dict) -> str | None:
        try:
            from anthropic import AsyncAnthropic
        except ImportError:
            logger.error("ClaudeChat: anthropic package not installed")
            return None

        api_key = cfg.get("api_key", "")
        model = cfg.get("model") or "claude-haiku-4-5-20251001"
        system_prompt = cfg.get("system_prompt") or "You are a helpful assistant on a Meshtastic radio mesh. Keep replies short."
        max_history = int(cfg.get("max_history") or 20)

        history = self._histories[sender_num]
        if history.maxlen != max_history:
            self._histories[sender_num] = deque(history, maxlen=max_history)
            history = self._histories[sender_num]

        history.append({"role": "user", "content": message})

        try:
            client = AsyncAnthropic(api_key=api_key)
            response = await client.messages.create(
                model=model,
                max_tokens=300,
                system=system_prompt,
                messages=list(history),
            )
            reply = response.content[0].text.strip()
            history.append({"role": "assistant", "content": reply})
            return reply
        except Exception:
            logger.exception("ClaudeChat: Claude API call failed")
            return None
