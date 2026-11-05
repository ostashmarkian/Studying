import asyncio
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.db import (
    DATABASE_CONNECTION,
    data_recording,
    init_db,
    mark_gone,
    read_boundary,
    stale_active,
    touch_seen,
    write_boundary,
)


SOURCE = "auto.ria"

A = "https://auto.ria.com/api/search/auto"
U = "https://auto.ria.com/graphql/"
H = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": "https://auto.ria.com/uk/search/",
    "Origin": "https://auto.ria.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
}
Q = """query($ids:[ID],$lang:ID){ advertisements(ids:$ids, langId:$lang){
  id createdAt year race VIN uri title custom abroad status
  brand{id name} model{id name} body{id} category{id name}
  fuel{id name} gearbox{name}
  engine{volume{liters} power{hp kW}}
  price{main{value currency{sign}} all{USD{value} UAH{value} EUR{value}}}
  location{city{id name} state{id name}}
  photos{main{url} all{url}}
  owner{id name rating{average count} company{id name}}
  publication{createdAt expiredAt}
  levels{active{value}} country{id}
}}"""

QT = "query($ids:[ID],$lang:ID){advertisements(ids:$ids,langId:$lang){id createdAt}}"

LIMIT = 500
URL = "https://auto.ria.com/uk/search/"
TIMEOUT = httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=60.0)
LIMITS = httpx.Limits(max_connections=8, max_keepalive_connections=8)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en-US;q=0.7,en;q=0.6",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-Ch-Ua": '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Referer": "https://auto.ria.com/uk/",
}

QUEUE_MAXSIZE = 8
GQL_BATCH = 500
CHUNKS_PER_BATCH = 5
CONSUMERS = 3
# Site ceiling: countpage above 100 is silently rounded back down to 100.
COUNTPAGE = 100
# How many pages an incremental run may walk without finding the boundary.
# Not an optimisation but a stopgap: without it any mistake in the boundary
# costs a full crawl, and that is over three thousand pages for one category.
MAX_PAGES = 200

queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)


async def request(client, method, url, retries=5, **kwargs):
    last = None
    for attempt in range(retries):
        try:
            r = await client.request(method, url, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                last = httpx.HTTPStatusError(
                    f"HTTP {r.status_code}", request=r.request, response=r
                )
            else:
                r.raise_for_status()
                return r
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            last = e
        delay = 2**attempt + random.uniform(0, 1)
        print(
            f"    retry {attempt + 1}/{retries} in {delay:.1f}s ({type(last).__name__})"
        )
        await asyncio.sleep(delay)
    raise last


async def page_time(client, car_id):
    """Bump time of a single listing.

    order_by=7 sorts the feed by exactly this field, so the time of the last id
    on a page tells us where we currently stand in the stream. The request is
    kept separate and deliberately narrow: one id, two fields.
    """
    r = await request(
        client,
        "POST",
        U,
        json={"query": QT, "variables": {"ids": [str(car_id)], "lang": 4}},
    )
    ads = (r.json().get("data") or {}).get("advertisements") or []
    stamp = ads[0].get("createdAt") if ads and ads[0] else None
    if not stamp:
        return None
    return datetime.fromtimestamp(int(stamp) / 1000, tz=timezone.utc)


async def fetch_ids(
    client,
    queue,
    categories=range(1, 11),
    full=False,
):
    """Collect ids from the feed and put them on the queue.

    The incremental boundary is a TIME, not the last recorded id. Feed order is
    set by bump time, so no single listing can be trusted as a marker: if it
    disappears the scan runs to the end of the site, and if it gets bumped the
    scan cuts off early and silently loses everything below it.
    """
    total = 0
    async with psycopg.AsyncConnection.connect(
        **DATABASE_CONNECTION, autocommit=True
    ) as conn:
        for category_id in categories:
            boundary = None if full else await read_boundary(conn, SOURCE, category_id)
            head_at = None
            reached = False
            page = 0
            while True:
                r = await request(
                    client,
                    "GET",
                    A,
                    params={
                        "category_id": category_id,
                        "page": page,
                        "countpage": COUNTPAGE,
                        "order_by": 7,
                    },
                )
                batch = r.json()["result"]["search_result"]["ids"]
                if not batch:
                    reached = True
                    break

                # Take the head before handing the page over: this is the
                # next boundary, and it has to be the time the run STARTED, or
                # whatever appears while we walk down is lost to the next run.
                if head_at is None:
                    head_at = await page_time(client, batch[0])

                await queue.put(batch)
                total += len(batch)
                if full:
                    await touch_seen(conn, batch)
                print(
                    f"cat {category_id} page {page}: +{len(batch)} "
                    f"(queue {queue.qsize()})"
                )
                page += 1

                if boundary is not None:
                    tail_at = await page_time(client, batch[-1])
                    if tail_at is not None and tail_at <= boundary:
                        print(f"cat {category_id}: boundary at page {page - 1}")
                        reached = True
                        break
                    if page >= MAX_PAGES:
                        print(
                            f"cat {category_id}: hit the {MAX_PAGES} page "
                            "ceiling, no boundary in sight, needs --sweep"
                        )
                        break

            # Move the boundary only after a category closed honestly. A run
            # that hit the ceiling leaves the old one in place: otherwise the
            # gap it failed to collect would never be collected by anyone.
            if reached and head_at is not None:
                await write_boundary(conn, SOURCE, category_id, head_at)
        print(f"finished: {total} id")


async def drain(queue, chunks=CHUNKS_PER_BATCH):
    batch = await queue.get()
    if batch is None:
        return None
    ids = list(batch)
    for _ in range(chunks - 1):
        try:
            nxt = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if nxt is None:
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                await queue.put(None)
            break
        ids += nxt
    return ids


async def consume(client, queue):
    while True:
        ids = await drain(queue)
        if ids is None:
            break
        try:
            await fetch_cars(client, ids)
        except Exception as e:
            print(f"  consume skiped {len(ids)} id: {type(e).__name__}: {str(e)[:70]}")


async def produce(
    client, queue, categories=range(1, 11), consumers=CONSUMERS, full=False
):
    try:
        await fetch_ids(client, queue, categories, full=full)
    finally:
        for _ in range(consumers):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                await queue.put(None)


async def fetch_cars(client, ids):
    r = await request(
        client,
        "POST",
        U,
        json={"query": Q, "variables": {"ids": ids[:GQL_BATCH], "lang": 4}},
    )
    d = r.json()
    if "errors" in d:
        print("errors:", len(d["errors"]), d["errors"][0]["message"])
    cars = [
        car
        for car in (d.get("data") or {}).get("advertisements") or []
        if car and car.get("status") == "ACTIVE" and car.get("brand")
    ]
    written, skipped, historied = await data_recording(cars, source=SOURCE)
    print(f"  db:recorded {written}, skipped {skipped}, histored +{historied}")


async def recheck(client, ids):
    """Find out what happened to listings that vanished from the feed.

    The ACTIVE filter that fetch_cars applies is deliberately absent here: the
    inactive status is exactly what we are after. The API reports it honestly,
    ARCHIVED, REMOVED_BY_CRON and the like, so the reason needs no guessing.
    """
    r = await request(
        client,
        "POST",
        U,
        json={"query": Q, "variables": {"ids": [str(i) for i in ids], "lang": 4}},
    )
    d = r.json()
    cars = [
        car
        for car in (d.get("data") or {}).get("advertisements") or []
        if car and car.get("title")
    ]
    await data_recording(cars, source=SOURCE)
    answered = {int(car["id"]) for car in cars if car.get("id")}
    lost = [i for i in ids if int(i) not in answered]
    await mark_gone(lost, SOURCE)
    return len(cars), len(lost)


async def sweep(categories=range(1, 11)):
    """Full crawl that clears out sold listings. Meant to run weekly.

    Search returns live listings only, so a sold one never shows up in the feed
    at all: it cannot be found, only its absence can be noticed. Hence the
    order of work. Walk everything and keep last_seen_at fresh, after which the
    active rows with a stale last_seen_at are exactly the ones that went away.
    """
    started_at = datetime.now(timezone.utc)
    async with httpx.AsyncClient(
        headers=headers, limits=LIMITS, timeout=TIMEOUT
    ) as client:
        await init_db()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(produce(client, queue, categories, full=True))
            for _ in range(CONSUMERS):
                tg.create_task(consume(client, queue))

        missing = await stale_active(started_at, SOURCE, categories)
        print(f"sweep: {len(missing)} gone from the feed, checking statuses")
        closed = lost = 0
        for start in range(0, len(missing), GQL_BATCH):
            chunk = missing[start : start + GQL_BATCH]
            try:
                done, gone = await recheck(client, chunk)
            except Exception as e:
                print(f"  recheck skiped {len(chunk)}: {type(e).__name__}")
                continue
            closed += done
            lost += gone
            print(f"  recheck {closed + lost}/{len(missing)}")
        print(f"sweep: {closed} statuses refreshed, {lost} marked GONE")


async def main():
    try:
        async with httpx.AsyncClient(
            headers=headers, limits=LIMITS, timeout=TIMEOUT
        ) as client:
            await init_db()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(produce(client, queue))
                for _ in range(CONSUMERS):
                    tg.create_task(consume(client, queue))
    except KeyboardInterrupt:
        print("The scraper has been forcibly shut down")


if __name__ == "__main__":
    # --sweep: full crawl that clears out sold listings, run it weekly.
    # no flag: the ordinary incremental run up to the boundary.
    entry = sweep() if "--sweep" in sys.argv else main()
    asyncio.run(entry, loop_factory=asyncio.SelectorEventLoop)
