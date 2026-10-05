import asyncio
import logging
import random
import re
import sys

import aiohttp


SERVER = "http://127.0.0.1:8080"

MESSAGE_LENGTH = 20

CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789"

MESSAGE_RE = re.compile(
    r"^[a-z0-9]{20}$"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
)


def generate_message():

    while True:

        message = "".join(
            random.choice(CHARACTERS)
            for _ in range(MESSAGE_LENGTH)
        )

        if MESSAGE_RE.fullmatch(message):
            return message


async def chunk_generator(message, name):

    # Break the message into deliberately small chunks.
    position = 0

    while position < len(message):

        size = random.randint(1, 4)

        chunk = message[
            position:position + size
        ]

        position += size

        logging.info(
            "[%s] HTTP chunk -> %r",
            name,
            chunk,
        )

        yield chunk.encode()

        await asyncio.sleep(
            random.uniform(0.05, 0.4)
        )


async def run(name):

    url = (
        f"{SERVER}/stream"
        f"?user={name}"
    )

    timeout = aiohttp.ClientTimeout(
        total=None
    )

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:

        while True:

            message = generate_message()

            logging.info(
                "[%s] GENERATED: %s",
                name,
                message,
            )

            try:

                async with session.post(
                    url,
                    data=chunk_generator(
                        message,
                        name,
                    ),
                ) as response:

                    result = await response.text()

                    logging.info(
                        "[%s] SERVER: %s",
                        name,
                        result.strip(),
                    )

            except Exception as exc:

                logging.error(
                    "[%s] HTTP error: %s",
                    name,
                    exc,
                )

            # Random interval before next message.
            await asyncio.sleep(
                random.uniform(1.0, 4.0)
            )


if __name__ == "__main__":

    name = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "USER-A"
    )

    logging.getLogger().name = name

    asyncio.run(
        run(name)
    )
