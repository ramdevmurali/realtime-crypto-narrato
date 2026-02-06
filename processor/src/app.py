import asyncio

from .streaming_core import StreamProcessor


async def main():
    processor = StreamProcessor()
    try:
        await processor.start()
    except KeyboardInterrupt:
        processor.log.info("processor_shutdown_requested")
    finally:
        await processor.stop()


if __name__ == "__main__":
    asyncio.run(main())
