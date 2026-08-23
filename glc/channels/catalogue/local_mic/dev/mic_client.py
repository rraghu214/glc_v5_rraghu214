#!/usr/bin/env python
"""Local microphone capture client for GLC — the fifth channel.

GLC ships the local_mic *adapter* (VAD, STT on the way in, TTS on the way out)
but no capture client: CHANNEL_SPECS["local_mic"] says "Run a local microphone
capture client that sends audio frames to GLC". This is that client, built on
the same shape as telegram/dev/live_poll.py.

Turn-based on purpose. Press Enter, speak, it records a fixed window, transcribes,
sends, waits for the reply, and speaks it back. A push-to-talk stream would be
nicer but has more ways to fail on camera; this has almost none.

Flow per turn
-------------
    mic -> WAV bytes -> adapter.on_message()   [VAD, then STT]
        -> ChannelMessage -> GLC WebSocket -> S16
        -> ChannelReply -> adapter.send()      [TTS]
        -> speakers

Prerequisites
-------------
    uv pip install sounddevice numpy pyttsx3

    glc_v5/.env needs GROQ_API_KEY (free at console.groq.com) — STT
    prefer="default" routes to groq_whisper. TTS prefer="fallback" uses
    pyttsx3 locally and needs no key.

Run from the glc_v5 checkout so the glc package imports:

    cd C:/Raghu/MyLearnings/EAG_V3/S16-08082026/assignment/glc_v5
    uv run python ../S16Code/htmlcov/scripts/local_mic_client.py

    # if the wrong microphone is picked up
    uv run python ../S16Code/htmlcov/scripts/local_mic_client.py --list-devices
    uv run python ../S16Code/htmlcov/scripts/local_mic_client.py --device 2
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import pathlib
import platform
import sys
import tempfile
import wave
from pathlib import Path

import websockets
from dotenv import load_dotenv

from glc.channels.catalogue.local_mic.adapter import Adapter
from glc.channels.envelope import ChannelReply
from glc.config import get_or_create_install_token
from glc.security.pairing import get_pairing_store

# Pin the env file to the glc_v5 checkout root. load_dotenv() with no
# argument searches upward from this file, which finds the wrong .env
# when the script is run from another repository.
load_dotenv(pathlib.Path(__file__).resolve().parents[5] / ".env")

# 16 kHz mono int16: what Whisper-family models expect, and the adapter's
# silence check requires sampwidth == 2 (see _wav_pcm_frames).
SAMPLE_RATE = 16_000
CHANNELS = 1
SPEAKER_ID = os.getenv("LOCAL_MIC_OWNER_ID", "local-owner")


def record_wav(seconds: float, device: int | None) -> bytes:
    """Block for *seconds*, return a complete WAV container."""
    import numpy  # noqa: F401  - sounddevice needs it for the array API
    import sounddevice

    frames = sounddevice.rec(
        int(seconds * SAMPLE_RATE), samplerate=SAMPLE_RATE,
        channels=CHANNELS, dtype="int16", device=device,
    )
    sounddevice.wait()

    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(CHANNELS)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(frames.tobytes())
    return buffer.getvalue()


def play_wav(audio: bytes, sample_rate: int) -> None:
    """Play the synthesized reply. winsound is stdlib and handles WAV
    containers directly, so Windows needs no playback dependency."""
    if not audio:
        return
    if platform.system() == "Windows":
        import winsound

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            handle.write(audio)
            path = handle.name
        try:
            winsound.PlaySound(path, winsound.SND_FILENAME)
        finally:
            Path(path).unlink(missing_ok=True)
        return
    try:
        import numpy
        import sounddevice

        with wave.open(io.BytesIO(audio), "rb") as wav:
            data = numpy.frombuffer(wav.readframes(wav.getnframes()), dtype=numpy.int16)
            sounddevice.play(data, wav.getframerate())
            sounddevice.wait()
    except Exception as error:  # noqa: BLE001
        print(f"[mic] could not play reply ({error!r}); text is above")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seconds", type=float, default=6.0, help="recording window per turn")
    parser.add_argument("--device", type=int, default=None, help="input device index")
    parser.add_argument("--list-devices", action="store_true")
    parser.add_argument("--stt", default="default", choices=["default", "local"],
                        help="default=groq_whisper (needs GROQ_API_KEY), local=whisper_cpp")
    args = parser.parse_args()

    if args.list_devices:
        import sounddevice

        print(sounddevice.query_devices())
        return

    if args.stt == "default" and not os.getenv("GROQ_API_KEY", "").strip():
        sys.exit("GROQ_API_KEY is empty in glc_v5/.env. Get a free key at console.groq.com, "
                 "or pass --stt local to use whisper_cpp.")

    # The speaker id is ours to choose, so pairing is unconditional -- unlike
    # Telegram or Discord there is no inbound identity to discover first.
    # Without owner_paired, s16code/routes.py grants no side effects at all.
    get_pairing_store().force_pair_owner("local_mic", SPEAKER_ID, user_handle="owner")
    print(f"[mic] paired owner: {SPEAKER_ID}")

    adapter = Adapter(config={
        "stt_prefer": args.stt,
        "tts_prefer": "fallback",   # system_fallback is the only implemented TTS
        "is_public_channel": False,
    })

    port = os.getenv("GLC_PORT", "8111")
    ws_url = f"ws://localhost:{port}/v1/channels/local_mic?token={get_or_create_install_token()}"
    print(f"[mic] connecting to GLC gateway on port {port} …")

    # This client is turn-based: it waits at an input() prompt between turns, and
    # a person taking a minute to think is the normal case. The websockets
    # default (ping every 20s, give up 20s later) reads that ordinary pause as a
    # dead peer and closes with "1011 keepalive ping timeout", killing the
    # session mid-conversation. Idle is expected here, so keepalive is off.
    async with websockets.connect(ws_url, ping_interval=None, close_timeout=5) as ws:
        print("[mic] connected.\n")
        print("=" * 60)
        print("  Press ENTER to speak. Ctrl-C to quit.")
        print(f"  Each turn records {args.seconds:.0f}s at {SAMPLE_RATE} Hz.")
        print("=" * 60 + "\n")

        while True:
            await asyncio.to_thread(input, "[mic] ENTER to record > ")
            print(f"[mic] recording {args.seconds:.0f}s … speak now")
            wav_bytes = await asyncio.to_thread(record_wav, args.seconds, args.device)
            print("[mic] transcribing …")

            try:
                message = await adapter.on_message(
                    {"wav_bytes": wav_bytes, "speaker_id": SPEAKER_ID,
                     "speaker_handle": "owner", "mime": "audio/wav",
                     "sample_rate": SAMPLE_RATE}
                )
            except Exception as error:  # noqa: BLE001
                print(f"[mic] STT failed: {error!r}\n")
                continue

            # The adapter returns None for silence, failed STT, or an empty
            # transcript. Silence is the common one -- it is a VAD drop, not a bug.
            if message is None:
                print("[mic] nothing heard (silence or empty transcript). Try again, "
                      "or lower vad_rms_threshold.\n")
                continue

            print(f'[mic] heard: "{message.text}"')
            await ws.send(message.model_dump_json())

            # The gateway channel WebSocket is request/response, so a plain
            # recv() is correct here -- no concurrent reader needed.
            payload = json.loads(await ws.recv())
            if "error" in payload:
                print(f"[mic] gateway error: {payload['error']}\n")
                continue

            reply = ChannelReply.model_validate(payload)
            print(f'[mic] reply: "{reply.text}"')
            try:
                spoken = await adapter.send(reply)
                # winsound.PlaySound blocks until the clip finishes; run on the
                # event loop it stalls the socket for the length of the reply.
                await asyncio.to_thread(play_wav, spoken.get("audio_bytes", b""),
                                        spoken.get("sample_rate", 22_050))
            except Exception as error:  # noqa: BLE001
                print(f"[mic] TTS failed: {error!r} (is pyttsx3 installed?)")
            print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[mic] shut down.")
