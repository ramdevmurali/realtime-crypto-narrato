import asyncio
from .app import StreamProcessor


def main():
    processor = StreamProcessor()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(processor.start())
    except KeyboardInterrupt:
        loop.run_until_complete(processor.stop())


if __name__ == "__main__":
    main()
