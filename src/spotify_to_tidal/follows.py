import asyncio
from functools import reduce
from typing import List, Sequence

import spotipy
import tidalapi
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm

from .requests import repeat_on_request_error, with_rate_limiter

from .type.spotify import SpotifyArtist, SpotifyTrack


def get_spotify_artist_follows(spotify_session: spotipy.Spotify) -> Sequence[SpotifyArtist]:
    print('Getting artist follows from spotify...')
    results = spotify_session.current_user_followed_artists()['artists']
    artists: List[SpotifyArtist] = results['items']
    with tqdm(total=results['total']) as pbar:
        pbar.update(len(results['items']))
        while after := results['cursors']['after']:
            results = spotify_session.current_user_followed_artists(after=after)['artists']
            artists.extend(results['items'])
            pbar.update(len(results['items']))
    return artists


def normalize_artist_name(artist_name: str) -> str:
    normed = (
        artist_name.lower()
                   .replace(' and ', ' & ')
                   .replace('"', '')
    )
    if normed.startswith('the '):
        normed = normed[4:]
    return normed


def artists_match(spotify_artist: SpotifyArtist, tidal_artist: tidalapi.Artist) -> bool:
    names_match = normalize_artist_name(spotify_artist['name']) == normalize_artist_name(tidal_artist.name)
    return names_match


def tracks_match(spotify_track: SpotifyTrack, tidal_track: tidalapi.Track) -> bool:
    return spotify_track['name'].lower() == tidal_track.name.lower()


async def match_artist_from_top_songs(
    spotify_artist: SpotifyArtist,
    spotify_session: spotipy.Spotify,
    tidal_session: tidalapi.Session,
    spotify_semaphore: asyncio.Semaphore,
    tidal_semaphore: asyncio.Semaphore,
) -> str | None:
    await spotify_semaphore.acquire()
    results = repeat_on_request_error(spotify_session.artist_top_tracks, spotify_artist['id'])
    top_tracks: Sequence[SpotifyTrack] = results['tracks']
    artist_name = spotify_artist['name']
    artist_matches = []

    for spotify_track in top_tracks:
        query = f"{normalize_artist_name(artist_name)} {spotify_track['name']}"
        await tidal_semaphore.acquire()
        results = repeat_on_request_error(tidal_session.search, query, models=[tidalapi.Track], limit=5)
        tidal_tracks = results['tracks']
        for tidal_track in tidal_tracks:
            if artists_match(spotify_artist, tidal_track.artist) and tracks_match(spotify_track, tidal_track):
                artist_matches.append(tidal_track.artist)

    if not len(artist_matches):
        return None

    # get the artist with the most matches
    def _get_artist_counts(acc: dict[str, int], artist: tidalapi.Artist):
        if artist.id not in acc:
            acc[artist.id] = 0
        acc[artist.id] += 1
        return acc
    artist_counts = reduce(_get_artist_counts, artist_matches, {})
    num_top_matches = max(artist_counts.values())
    top_matches = [ aid for aid, cnt in artist_counts.items() if cnt == num_top_matches ]
    if len(top_matches) != 1:
        return None
    return top_matches[0]


async def get_tidal_artist(
    spotify_artist: SpotifyArtist,
    spotify_session: spotipy.Spotify,
    tidal_session: tidalapi.Session,
    spotify_semaphore: asyncio.Semaphore,
    tidal_semaphore: asyncio.Semaphore,
) -> str | None:
    name_input = spotify_artist['name']
    if name_input.lower().startswith('the '):
        name_input = name_input[4:]
    await tidal_semaphore.acquire()
    results = repeat_on_request_error(tidal_session.search, name_input, models=[tidalapi.Artist])
    tidal_artist_matches = [
        tidal_artist
        for tidal_artist
        in results['artists']
        if artists_match(spotify_artist, tidal_artist)
    ]
    # if there's only one match, we're done
    if len(tidal_artist_matches) == 1:
        return tidal_artist_matches[0].id
    # if there are multiple or none, search top songs
    tidal_artist_matches = await match_artist_from_top_songs(spotify_artist, spotify_session, tidal_session, spotify_semaphore, tidal_semaphore)
    return tidal_artist_matches


@with_rate_limiter
@with_rate_limiter
async def search_tidal_artists(
    spotify_artists: Sequence[SpotifyArtist],
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    spotify_semaphore: asyncio.Semaphore,
    tidal_semaphore: asyncio.Semaphore
) -> Sequence[tidalapi.Artist]:
    task_description = f"Searching Tidal for {len(spotify_artists)} artists..."
    tidal_artists = await atqdm.gather(*[
        get_tidal_artist(artist, spotify_session, tidal_session, spotify_semaphore, tidal_semaphore)
        for artist in spotify_artists
    ], desc=task_description)
    return tidal_artists


# async def follow_tidal_artist(tidal_artist: tidalapi.Artist, tidal_favs: tidalapi.Favorites, semaphore: asyncio.Semaphore):
#     await semaphore.acquire()
#     tidal_favs.add_artist(tidal_artist.id)


@with_rate_limiter
async def follow_tidal_artists(artist_ids: Sequence[str], tidal_session: tidalapi.Session, semaphore: asyncio.Semaphore):
    favs = tidalapi.Favorites(tidal_session, tidal_session.user.id)
    for artist_id in tqdm(artist_ids):
        await semaphore.acquire()
        favs.add_artist(artist_id)


def sync_artist_follows(args, spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config: dict):
    spotify_artists = get_spotify_artist_follows(spotify_session)
    tidal_artists = asyncio.run(
        search_tidal_artists(spotify_artists, tidal_session, spotify_session)
    )

    artists_to_follow = list(filter(None, tidal_artists))
    no_matches = [
        spotify_artist['name']
        for spotify_artist, tidal_artist
        in zip(spotify_artists, tidal_artists)
        if tidal_artist is None
    ]

    asyncio.run(
        follow_tidal_artists(artists_to_follow, tidal_session)
    )

    if len(no_matches):
        print('Artists without matches:', end='\n\t')
        print('\n\t'.join(no_matches))
