#!/usr/bin/env python
"""IMAP polling bridge for GLC — the one piece the catalogue does not ship.

Every other channel you need has a runnable bridge in glc_v5:

    whatsapp/demo_webhook_server.py        telegram/dev/live_poll.py
    discord/tests/run_discord_bridge.py    twilio_sms/server.py

IMAP has the adapter, the connection manager, the MIME parser, the SMTP sender
and the UID tracker -- but no loop tying them together. This is that loop,
modelled directly on telegram/dev/live_poll.py so it behaves the same way.

What it does
------------
  1. SELECTs exactly one mailbox folder and watches only that folder.
  2. SEARCH UNSEEN -> FETCH RFC822 on a poll interval.
  3. adapter.on_message() -> ChannelMessage -> GLC WebSocket.
  4. ChannelReply from GLC -> adapter.send() -> SMTP.
  5. Marks each message \\Seen only after it has been forwarded.

Folder scoping is real, not advisory: ImapConnection.connect() calls
select(self.mailbox) and the session is bound to that folder for its lifetime.
Point `mailbox` at a dedicated label and your main inbox is never selected.

Run this from the glc_v5 checkout so the glc package is importable:

    cd C:/Raghu/MyLearnings/EAG_V3/S16-08082026/assignment/glc_v5
    uv run python ../S16Code/htmlcov/scripts/imap_live_poll.py

Environment (put these in glc_v5/.env):

    IMAP_HOST=imap.gmail.com          IMAP_PORT=993
    IMAP_USER=you@gmail.com           IMAP_PASSWORD=<16-char app password>
    IMAP_MAILBOX=EA-Watch             # a label, NOT INBOX
    SMTP_HOST=smtp.gmail.com          SMTP_PORT=587
    SMTP_USER=you@gmail.com           SMTP_PASSWORD=<same app password>
    IMAP_BOT_FROM=you@gmail.com
    IMAP_OWNER_EMAIL=you@gmail.com    # paired as owner; omit to auto-pair first sender
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys

import websockets
from dotenv import load_dotenv

from glc.channels.catalogue.imap.adapter import Adapter
from glc.channels.catalogue.imap.connection import ImapConnection
from glc.channels.envelope import ChannelReply
from glc.config import get_or_create_install_token
from glc.security.pairing import get_pairing_store

# Pin the env file to the glc_v5 checkout root. load_dotenv() with no
# argument searches upward from this file, which finds the wrong .env
# when the script is run from another repository.
load_dotenv(pathlib.Path(__file__).resolve().parents[5] / ".env")

POLL_SECONDS = float(os.getenv("IMAP_POLL_SECONDS", "15"))


def refresh_view(connection) -> None:
    """Ask the server for news, so SEARCH sees mail that arrived after SELECT.

    A SELECTed IMAP session does not learn about new mail on its own -- the
    server only reports EXISTS changes in reply to a command. Without this the
    connection keeps searching the snapshot it had at connect time, and the
    poller sits there looking perfectly healthy while never seeing anything.
    """
    conn = getattr(connection, "_conn", None)
    if conn is not None:
        conn.noop()


def build_config() -> dict:
    """Adapter config. GLC does not read IMAP env vars itself — the bridge
    injects them, which is why this file has to exist at all."""
    return {
        "imap_host": os.getenv("IMAP_HOST", "imap.gmail.com"),
        "imap_port": int(os.getenv("IMAP_PORT", "993")),
        "imap_user": os.getenv("IMAP_USER", ""),
        "imap_password": os.getenv("IMAP_PASSWORD", ""),
        "mailbox": os.getenv("IMAP_MAILBOX", "INBOX"),
        "smtp_host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port": int(os.getenv("SMTP_PORT", "587")),
        "smtp_user": os.getenv("SMTP_USER", os.getenv("IMAP_USER", "")),
        "smtp_password": os.getenv("SMTP_PASSWORD", os.getenv("IMAP_PASSWORD", "")),
        "bot_from": os.getenv("IMAP_BOT_FROM", os.getenv("IMAP_USER", "")),
        "default_subject": "Your assistant",
        "is_public_channel": False,
    }


async def main() -> None:
    config = build_config()
    if not config["imap_user"] or not config["imap_password"]:
        sys.exit("IMAP_USER and IMAP_PASSWORD must be set (use an app password, never your login password)")

    if config["mailbox"].upper() == "INBOX" and os.getenv("IMAP_ALLOW_INBOX", "").strip() != "1":
        # Loud on purpose when the account is a personal one: watching INBOX
        # hands the whole mail stream to the agent. On a dedicated account that
        # is the intended setup, so IMAP_ALLOW_INBOX=1 silences this.
        print("!! WARNING: mailbox is INBOX. Fine on a dedicated account — set "
              "IMAP_ALLOW_INBOX=1 to silence. On a personal account, point "
              "IMAP_MAILBOX at a label instead.")

    store = get_pairing_store()
    owner = os.getenv("IMAP_OWNER_EMAIL", "").strip()
    if owner:
        store.force_pair_owner("imap", owner, user_handle="owner")
        print(f"[imap] paired owner: {owner}")
    else:
        print("[imap] no IMAP_OWNER_EMAIL set — will auto-pair the first sender seen")

    adapter = Adapter(config=config)
    connection = ImapConnection(
        host=config["imap_host"], port=config["imap_port"],
        user=config["imap_user"], password=config["imap_password"],
        mailbox=config["mailbox"],
    )

    print(f"[imap] connecting to {config['imap_host']}:{config['imap_port']} …")
    await asyncio.to_thread(connection.connect)
    print(f"[imap] watching folder: {config['mailbox']}")

    port = os.getenv("GLC_PORT", "8111")
    ws_url = f"ws://localhost:{port}/v1/channels/imap?token={get_or_create_install_token()}"
    print(f"[imap] connecting to GLC gateway on port {port} …")

    # Same reasoning as the mic client: long idle gaps between mails are normal,
    # and the library's default keepalive would read one as a dead peer.
    async with websockets.connect(ws_url, ping_interval=None, close_timeout=5) as ws:
        print("[imap] connected. Polling for unseen mail.\n")

        async def poll_mailbox() -> None:
            nonlocal owner
            while True:
                try:
                    await asyncio.to_thread(refresh_view, connection)
                    # imaplib is blocking; keep it off the event loop so
                    # outbound replies are never stalled behind a fetch.
                    messages = await asyncio.to_thread(connection.fetch_unseen)
                    for envelope in messages:
                        message = await adapter.on_message(envelope)
                        if message is None:
                            print(f"[imap] uid {envelope['uid']} dropped (unparseable or untrusted)")
                            await asyncio.to_thread(connection.mark_seen, envelope["uid"])
                            continue

                        if not owner and message.channel_user_id:
                            owner = message.channel_user_id
                            store.force_pair_owner("imap", owner, user_handle="owner")
                            print(f"\n*** auto-paired {owner} as owner ***\n")

                        print(f"[imap] uid {envelope['uid']} from {message.channel_user_id}: "
                              f"{(message.text or '')[:80]!r}")
                        await ws.send(message.model_dump_json())
                        # Only after a successful forward. A crash mid-send
                        # leaves it unseen and it is retried, rather than
                        # silently swallowed.
                        await asyncio.to_thread(connection.mark_seen, envelope["uid"])
                except Exception as error:  # noqa: BLE001 - a poller must not die
                    print(f"[imap] poll error: {error!r}; reconnecting")
                    try:
                        await asyncio.to_thread(connection.reconnect)
                    except Exception as reconnect_error:  # noqa: BLE001
                        print(f"[imap] reconnect failed: {reconnect_error!r}")
                await asyncio.sleep(POLL_SECONDS)

        async def relay_replies() -> None:
            while True:
                try:
                    payload = json.loads(await ws.recv())
                    if "error" in payload:
                        print(f"[imap] gateway error: {payload['error']}")
                        continue
                    reply = ChannelReply.model_validate(payload)
                    print(f"[imap] replying to {reply.channel_user_id}: {(reply.text or '')[:80]!r}")
                    await adapter.send(reply)
                except websockets.exceptions.ConnectionClosed:
                    print("[imap] gateway connection closed")
                    break
                except Exception as error:  # noqa: BLE001
                    print(f"[imap] reply error: {error!r}")

        try:
            await asyncio.gather(poll_mailbox(), relay_replies())
        finally:
            await asyncio.to_thread(connection.close)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[imap] shut down.")
