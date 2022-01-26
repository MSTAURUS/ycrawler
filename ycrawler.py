import argparse
import asyncio
import hashlib
import logging
import sys
import time
from mimetypes import guess_extension
from pathlib import Path
from typing import Dict, List

import aiofiles
import aiohttp
import requests as req
from bs4 import BeautifulSoup

URL = "https://news.ycombinator.com/"
COMMENTS_URL = URL+"item?id="
NEWS_PATH = "output/"
DB = Path(NEWS_PATH, "db")
REPEAT = 5
TIMEOUT = 300
LOG = "Log.log"
HEADERS = "Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0"


async def get_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"User-Agent": HEADERS}) as response:
            response.raise_for_status()
            content = await response.read()
            return content, response.headers["CONTENT-TYPE"]


async def check_news(hash_news: str) -> bool:
    try:
        result = True
        hash_news = str(hash_news)
        if not DB.exists():
            # Creating dir+file in write to disk
            return result

        async with aiofiles.open(DB, mode='r') as f:
            contents = await f.read()
            if hash_news in contents:
                result = False
        return result
    except IOError as err:
        logging.exception("I/O error({0}): {1}. The default config is used ".format(str(err.errno), err.strerror))
        return result


async def write_news_id(id_news: str) -> None:
    try:
        if id_news:
            path = Path(opts.news)
            id_news = str(id_news)
            if not path.exists():
                path.mkdir(exist_ok=True, parents=True)
            async with aiofiles.open(DB, mode='a') as f:
                await f.write(id_news+"\n")
    except IOError as err:
        logging.exception("I/O error({0}): {1}. The default config is used ".format(err.errno, err.strerror))


async def get_request(url: str) -> str:
    count = 0
    for count in range(REPEAT):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"User-Agent": HEADERS}) as response:
                    data = await response.text()
                    if response.status == 200:
                        break
        except Exception:
            logging.exception("Try get again %i" % count)

    if count == 4:
        logging.exception("Thank you Mario! But your princess is in another castle")
    else:
        return data


async def get_index_page(url: str) -> Dict:
    result = {}

    request = await get_request(url)
    if request:
        soup = BeautifulSoup(request, "lxml")

        table = soup.body.select(".itemlist")
        tr = table[0].find_all("tr", class_="athing")

        result = {link.find("a", class_="titlelink").get_text(): COMMENTS_URL+link["id"] for link in tr}

        return result
    else:
        return result


async def get_children_links(url: str) -> List:
    result = []

    request = await get_request(url)

    if request:
        soup = BeautifulSoup(request, "lxml")

        comments = soup.body.select(".commtext")

        for comment in comments:
            link = comment.find("a")
            if link:
                result.append(link["href"])

        return result
    else:
        return []


async def save_on_disk(url: str, path_name: Path) -> None:
    if not path_name.exists():
        path_name.mkdir(exist_ok=True, parents=True)

    try:
        content, content_type = await get_page(url)
        suffix = guess_extension(content_type.partition(';')[0].strip())
        filename = path_name.parts[-1]+suffix

        async with aiofiles.open(path_name / filename, mode='wb') as f:
            await f.write(content)
            await f.close()
    except Exception as err:
        logging.exception("Error save link %s with mesage %s" % (url, err))


async def main():
    while True:
        main_page = await get_index_page(URL)

        try:
            for news_name in main_page:
                link = main_page[news_name]
                news_name = news_name.replace("/", "-").replace(".", "_")
                path = Path(opts.news, news_name)
                hash_str = news_name+link
                hash_str = hash_str.encode()
                hash_str = hashlib.md5(hash_str)
                hash_news = hash_str.hexdigest()

                new_news = await check_news(hash_news)

                # yes, we new
                if new_news:
                    await save_on_disk(link, path)

                    children_lnk = await get_children_links(link)
                    for count, link in enumerate(children_lnk):
                        children_path = Path(path, str(count))
                        await save_on_disk(link, children_path)

                    # for memory use
                    await write_news_id(hash_news)
        except Exception as err:
            logging.exception("Error: %s" % err)
        finally:
            logging.info("Do you know that feeling when you don't know if you're awake or asleep?")
            await asyncio.sleep(opts.timeout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-db", "--database", default=DB, help="System base path")
    parser.add_argument("-t", "--timeout", default=TIMEOUT, help="TIMEOUT time", type=int)
    parser.add_argument("-n", "--news", default=NEWS_PATH, help="Path for news")
    parser.add_argument("-l", "--log", default=LOG, help="Path for logfile")
    opts = parser.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )

    loop = asyncio.get_event_loop()
    DB = Path(opts.news, "db")

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Terminate")
    except Exception as e:
        logging.error("Error: %s" % e)
        sys.exit(1)
    finally:
        loop.close()
