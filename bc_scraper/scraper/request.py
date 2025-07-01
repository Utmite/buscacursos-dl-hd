import os
import traceback
from typing import Callable, Dict
import requests
from time import sleep
import logging
import hashlib
import json
import binascii
import multiprocessing

log = logging.getLogger("scraper")

# Lock global para cache y archivo
_file_lock = multiprocessing.Lock()

cache: Dict[str, str] = {}
cachefile = None


def load_cache():
    global cachefile
    if os.path.exists(".requestcache"):
        with open(".requestcache", "r") as file:
            for line in file:
                try:
                    c = json.loads(line)
                    cache[c["key"]] = c["resp"]
                except json.JSONDecodeError:
                    continue

    # Abrimos el archivo en modo append
    cachefile = open(".requestcache", "a", encoding="utf-8")


def add_to_cache(key: str, resp: str):
    # Protegemos tanto el dict como el archivo
    with _file_lock:
        cache[key] = resp
        if cachefile:
            json.dump({"key": key, "resp": resp}, cachefile, ensure_ascii=False)
            cachefile.write("\n")
            cachefile.flush()


def get_text_raw(cfg, url: str, key: str, fetchtext: Callable[[], str]):
    if not cfg.get("disable-cache"):
        with _file_lock:
            if key in cache:
                log.info("request to %s hit cache", url)
                return cache[key]

    tries = 10
    while tries > 0:
        try:
            resp = fetchtext()
            if not cfg.get("disable-cache"):
                add_to_cache(key, resp)
            return resp
        except Exception:
            log.error(f"request to {url} failed:")
            log.error(traceback.format_exc())
            tries -= 1
            sleep(1)
            if tries > 0:
                log.info("retrying...")
    raise Exception(f'too many tries to URL "{url}"')


def make_key(obj) -> str:
    return binascii.hexlify(
        hashlib.blake2b(json.dumps(obj, sort_keys=True).encode("utf-8")).digest()
    ).decode("ascii")


def get_text(cfg, query: str) -> str:
    cookies = cfg.get("cookies", "")
    key = make_key(
        {
            "m": "get",
            "url": query,
            "cks": cookies,
        }
    )

    def fetch():
        return requests.get(query, headers={"Cookie": cookies}).text

    return get_text_raw(cfg, query, key, fetch)


def post_text(cfg, url: str, form_params: Dict[str, str]) -> str:
    cookies = cfg.get("cookies", "")
    key = make_key(
        {
            "m": "post",
            "url": url,
            "cks": cookies,
            "prm": form_params,
        }
    )

    def fetch():
        return requests.post(url, data=form_params, headers={"Cookie": cookies}).text

    return get_text_raw(cfg, f"{url} & {form_params}", key, fetch)
