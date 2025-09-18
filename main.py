import asyncio
from bot.dispatcher import start_bot


def main():
    asyncio.run(start_bot())


if __name__ == '__main__':
    main()
