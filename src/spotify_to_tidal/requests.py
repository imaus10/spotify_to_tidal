import asyncio
import datetime
import math
import sys
import time
import traceback
from typing import Awaitable, Callable, List

import requests
import spotipy
import tidalapi
from tqdm.asyncio import tqdm as atqdm


async def repeat_on_request_error(function: Callable, *args, remaining=5, **kwargs):
    # utility to repeat calling the function up to 5 times if an exception is thrown
    try:
        return await asyncio.to_thread(function, *args, **kwargs)
    except (tidalapi.exceptions.TooManyRequests, requests.exceptions.RequestException, spotipy.exceptions.SpotifyException) as e:
        if remaining:
            print(f"{str(e)} occurred, retrying {remaining} times")
        else:
            print(f"{str(e)} could not be recovered")

        if isinstance(e, requests.exceptions.RequestException) and e.response is not None:
            print(f"Response message: {e.response.text}")
            print(f"Response headers: {e.response.headers}")

        if not remaining:
            print("Aborting sync")
            print(f"The following arguments were provided:\n\n {str(args)}")
            print(traceback.format_exc())
            sys.exit(1)
        sleep_schedule = {5: 1, 4:10, 3:60, 2:5*60, 1:10*60} # sleep variable length of time depending on retry number
        asyncio.sleep(sleep_schedule.get(remaining, 1))
        return await repeat_on_request_error(function, *args, remaining=remaining-1, **kwargs)


async def fetch_all_from_spotify_in_chunks(
        fetch_function,
        *args,
        is_playlist: bool = True,
        **kwargs,
) -> List[dict]:
    def _get_tracks_from_playlist_results(playlist_results):
        return [item['track'] for item in playlist_results['items'] if item['track'] is not None]
    def _get_items_from_results(album_results):
        return album_results['items']
    get_tracks = _get_tracks_from_playlist_results if is_playlist else _get_items_from_results

    output = []
    results = await fetch_function(*args, **kwargs, offset=0)
    output.extend(get_tracks(results))

    # Get all the remaining tracks in parallel
    if results['next']:
        offsets = [results['limit'] * n for n in range(1, math.ceil(results['total'] / results['limit']))]
        extra_results = await atqdm.gather(
            *[ fetch_function(*args, **kwargs, offset=offset) for offset in offsets ],
            desc="Fetching additional data chunks"
        )
        for extra_result in extra_results:
            output.extend(get_tracks(extra_result))

    return output


async def _run_rate_limiter(semaphore: asyncio.Semaphore, max_concurrency: int, rate_limit: int):
    '''
    Leaky bucket algorithm for rate limiting. 
    Periodically releases items from semaphore at rate_limit
    '''
    _sleep_time = max_concurrency/rate_limit/4 # aim to sleep approx time to drain 1/4 of 'bucket'
    t0 = datetime.datetime.now()
    while True:
        await asyncio.sleep(_sleep_time)
        t = datetime.datetime.now()
        dt = (t - t0).total_seconds()
        new_items = round(rate_limit*dt)
        t0 = t
        # leak new_items from the 'bucket'
        for _ in range(new_items):
            semaphore.release()


def with_rate_limiter(func: Awaitable, max_concurrency=10, rate_limit=10):
    # max_concurrency = config.get('max_concurrency', 10)
    # rate_limit = config.get('rate_limit', 10)
    async def _with_rate_limiter(*args, **kwargs):
        semaphore = asyncio.Semaphore(max_concurrency)
        rate_limiter_task = asyncio.create_task(_run_rate_limiter(semaphore, max_concurrency, rate_limit))
        results = await func(*args, semaphore, **kwargs)
        rate_limiter_task.cancel()
        return results
    return _with_rate_limiter
