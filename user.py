import asyncio
import json
import logging
import queue
import sys
import threading
import tkinter as tk

import aiohttp


SERVER = "http://127.0.0.1:8080"


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [USER] %(message)s",
)

log = logging.getLogger("user")


# ------------------------------------------------------------
# User identity
# ------------------------------------------------------------

USER_NAME = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "USER-A"
)


# ------------------------------------------------------------
# Thread communication
# ------------------------------------------------------------

events = queue.Queue()


# ------------------------------------------------------------
# Async network worker
# ------------------------------------------------------------

async def websocket_loop():

    url = (
        SERVER.replace(
            "http://",
            "ws://"
        )
        + "/ws"
    )

    while True:

        try:

            log.info(
                "Connecting WebSocket: %s",
                url,
            )

            async with aiohttp.ClientSession() as session:

                async with session.ws_connect(
                    url
                ) as ws:

                    log.info(
                        "WebSocket connected"
                    )

                    async for msg in ws:

                        if msg.type == aiohttp.WSMsgType.TEXT:

                            try:
                                data = json.loads(
                                    msg.data
                                )

                                events.put(
                                    (
                                        "chat",
                                        data,
                                    )
                                )

                                log.info(
                                    "WS <- %s",
                                    data,
                                )

                            except Exception as exc:

                                log.error(
                                    "Bad WS JSON: %s",
                                    exc,
                                )

        except Exception as exc:

            log.error(
                "WebSocket error: %s",
                exc,
            )

        await asyncio.sleep(2)


async def sse_loop():

    url = SERVER + "/thinking"

    while True:

        try:

            log.info(
                "Connecting SSE: %s",
                url,
            )

            async with aiohttp.ClientSession() as session:

                async with session.get(
                    url,
                    headers={
                        "Accept":
                        "text/event-stream"
                    },
                ) as response:

                    log.info(
                        "SSE connected"
                    )

                    buffer = ""

                    async for chunk in response.content.iter_chunked(128):

                        buffer += chunk.decode(
                            errors="replace"
                        )

                        while "\n\n" in buffer:

                            event, buffer = buffer.split(
                                "\n\n",
                                1,
                            )

                            for line in event.splitlines():

                                if line.startswith(
                                    "data:"
                                ):

                                    raw = line[
                                        5:
                                    ].strip()

                                    try:

                                        data = json.loads(
                                            raw
                                        )

                                        events.put(
                                            (
                                                "thinking",
                                                data,
                                            )
                                        )

                                        log.info(
                                            "SSE <- %s",
                                            data,
                                        )

                                    except Exception:
                                        pass

        except Exception as exc:

            log.error(
                "SSE error: %s",
                exc,
            )

        await asyncio.sleep(2)


async def network_main():

    await asyncio.gather(
        websocket_loop(),
        sse_loop(),
    )


def network_thread():

    asyncio.run(
        network_main()
    )


# ------------------------------------------------------------
# Tkinter GUI
# ------------------------------------------------------------

class ChatWindow:

    def __init__(self, root):

        self.root = root

        root.title(
            f"HTTP/WebSocket/SSE Chat - {USER_NAME}"
        )

        root.geometry(
            "700x500"
        )

        # ----------------------------------------------------
        # User
        # ----------------------------------------------------

        tk.Label(
            root,
            text=USER_NAME,
            font=("Arial", 18, "bold"),
        ).pack(
            pady=(10, 5)
        )

        # ----------------------------------------------------
        # Current message
        # ----------------------------------------------------

        self.message = tk.StringVar(
            value="Waiting for message..."
        )

        self.message_label = tk.Label(
            root,
            textvariable=self.message,
            font=("Courier New", 24),
            relief="groove",
            bd=2,
            padx=15,
            pady=20,
        )

        self.message_label.pack(
            fill="x",
            padx=20,
            pady=10,
        )

        # ----------------------------------------------------
        # Thinking
        # ----------------------------------------------------

        self.thinking = tk.StringVar(
            value=""
        )

        tk.Label(
            root,
            textvariable=self.thinking,
            font=("Arial", 13, "italic"),
        ).pack(
            pady=5
        )

        # ----------------------------------------------------
        # Log
        # ----------------------------------------------------

        tk.Label(
            root,
            text="Log",
            anchor="w",
        ).pack(
            fill="x",
            padx=20,
        )

        self.log_box = tk.Text(
            root,
            height=15,
            state="disabled",
            font=("Courier New", 9),
        )

        self.log_box.pack(
            fill="both",
            expand=True,
            padx=20,
            pady=(0, 15),
        )

        root.after(
            100,
            self.process_events,
        )

        root.protocol(
            "WM_DELETE_WINDOW",
            self.close,
        )

    # --------------------------------------------------------
    # GUI event processor
    # --------------------------------------------------------

    def process_events(self):

        try:

            while True:

                event_type, data = events.get_nowait()

                if event_type == "chat":

                    user = data.get(
                        "user",
                        "?",
                    )

                    message = data.get(
                        "message",
                        "",
                    )

                    self.message.set(
                        message
                    )

                    self.add_log(
                        f"CHAT {user}: {message}"
                    )

                    # A real chat message temporarily
                    # replaces the thinking indicator.
                    self.thinking.set("")

                elif event_type == "thinking":

                    message = data.get(
                        "message",
                        "",
                    )

                    self.thinking.set(
                        message
                    )

                    self.add_log(
                        f"THINKING: {message}"
                    )

        except queue.Empty:
            pass

        self.root.after(
            100,
            self.process_events,
        )

    # --------------------------------------------------------
    # Logging in GUI
    # --------------------------------------------------------

    def add_log(self, text):

        self.log_box.configure(
            state="normal"
        )

        self.log_box.insert(
            "end",
            text + "\n",
        )

        self.log_box.see(
            "end"
        )

        self.log_box.configure(
            state="disabled"
        )

    def close(self):

        self.root.destroy()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

if __name__ == "__main__":

    thread = threading.Thread(
        target=network_thread,
        daemon=True,
    )

    thread.start()

    root = tk.Tk()

    ChatWindow(root)

    root.mainloop()
