import asyncio
import datetime
from difflib import SequenceMatcher
import math
from typing import List, Sequence, Set, Mapping
import unicodedata

import spotipy
import tidalapi
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm

from .cache import failure_cache, track_match_cache
from .requests import (
    fetch_all_from_spotify_in_chunks,
    repeat_on_request_error,
    with_rate_limiter,
)
from .tidalapi_patch import (
    add_multiple_tracks_to_playlist,
    clear_tidal_playlist,
    get_all_favorites,
    get_all_playlists,
    get_all_playlist_tracks
)
from .type import spotify as t_spotify


def normalize(s) -> str:
    return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('ascii')


def simple(input_string: str) -> str:
    # only take the first part of a string before any hyphens or brackets to account for different versions
    return input_string.split('-')[0].strip().split('(')[0].strip().split('[')[0].strip()


def isrc_match(tidal_track: tidalapi.Track, spotify_track) -> bool:
    if "isrc" in spotify_track["external_ids"]:
        return tidal_track.isrc == spotify_track["external_ids"]["isrc"]
    return False


def duration_match(tidal_track: tidalapi.Track, spotify_track, tolerance=2) -> bool:
    # the duration of the two tracks must be the same to within 2 seconds
    return abs(tidal_track.duration - spotify_track['duration_ms']/1000) < tolerance


def name_match(tidal_track, spotify_track) -> bool:
    def exclusion_rule(pattern: str, tidal_track: tidalapi.Track, spotify_track: t_spotify.SpotifyTrack):
        spotify_has_pattern = pattern in spotify_track['name'].lower()
        tidal_has_pattern = pattern in tidal_track.name.lower() or (not tidal_track.version is None and (pattern in tidal_track.version.lower()))
        return spotify_has_pattern != tidal_has_pattern

    # handle some edge cases
    if exclusion_rule("instrumental", tidal_track, spotify_track): return False
    if exclusion_rule("acapella", tidal_track, spotify_track): return False
    if exclusion_rule("remix", tidal_track, spotify_track): return False

    # the simplified version of the Spotify track name must be a substring of the Tidal track name
    # Try with both un-normalized and then normalized
    simple_spotify_track = simple(spotify_track['name'].lower()).split('feat.')[0].strip()
    return simple_spotify_track in tidal_track.name.lower() or normalize(simple_spotify_track) in normalize(tidal_track.name.lower())


def artist_match(tidal: tidalapi.Track | tidalapi.Album, spotify) -> bool:
    def split_artist_name(artist: str) -> Sequence[str]:
       if '&' in artist:
           return artist.split('&')
       elif ',' in artist:
           return artist.split(',')
       else:
           return [artist]

    def get_tidal_artists(tidal: tidalapi.Track | tidalapi.Album, do_normalize=False) -> Set[str]:
        result: list[str] = []
        for artist in tidal.artists:
            if do_normalize:
                artist_name = normalize(artist.name)
            else:
                artist_name = artist.name
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])

    def get_spotify_artists(spotify, do_normalize=False) -> Set[str]:
        result: list[str] = []
        for artist in spotify['artists']:
            if do_normalize:
                artist_name = normalize(artist['name'])
            else:
                artist_name = artist['name']
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])
    # There must be at least one overlapping artist between the Tidal and Spotify track
    # Try with both un-normalized and then normalized
    if get_tidal_artists(tidal).intersection(get_spotify_artists(spotify)) != set():
        return True
    return get_tidal_artists(tidal, True).intersection(get_spotify_artists(spotify, True)) != set()


def match(tidal_track, spotify_track) -> bool:
    if not spotify_track['id']: return False
    return isrc_match(tidal_track, spotify_track) or (
        duration_match(tidal_track, spotify_track)
        and name_match(tidal_track, spotify_track)
        and artist_match(tidal_track, spotify_track)
    )


def test_album_similarity(spotify_album, tidal_album, threshold=0.6):
    return (
        SequenceMatcher(None, simple(spotify_album['name']), simple(tidal_album.name)).ratio() >= threshold and
        artist_match(tidal_album, spotify_album)
    )


async def tidal_search(
    spotify_track: t_spotify.SpotifyTrack,
    rate_limiter: asyncio.Semaphore,
    tidal_session: tidalapi.Session
) -> tidalapi.Track | None:
    async def _search_for_track_in_album():
        # search for album name and first album artist
        if 'album' in spotify_track and 'artists' in spotify_track['album'] and len(spotify_track['album']['artists']):
            query = simple(spotify_track['album']['name']) + " " + simple(spotify_track['album']['artists'][0]['name'])
            album_result = await repeat_on_request_error(
                tidal_session.search, query, models=[tidalapi.album.Album]
            )
            for album in album_result['albums']:
                if album.num_tracks >= spotify_track['track_number'] and test_album_similarity(spotify_track['album'], album):
                    album_tracks = album.tracks()
                    if len(album_tracks) < spotify_track['track_number']:
                        assert( not len(album_tracks) == album.num_tracks ) # incorrect metadata :(
                        continue
                    track = album_tracks[spotify_track['track_number'] - 1]
                    if match(track, spotify_track):
                        failure_cache.remove_match_failure(spotify_track['id'])
                        return track

    async def _search_for_standalone_track():
        # if album search fails then search for track name and first artist
        query = simple(spotify_track['name']) + ' ' + simple(spotify_track['artists'][0]['name'])
        results = await repeat_on_request_error(tidal_session.search, query, models=[tidalapi.media.Track])
        for track in results['tracks']:
            if match(track, spotify_track):
                failure_cache.remove_match_failure(spotify_track['id'])
                return track

    await rate_limiter.acquire()
    album_search = await _search_for_track_in_album()
    if album_search:
        return album_search
    await rate_limiter.acquire()
    track_search = await _search_for_standalone_track()
    if track_search:
        return track_search

    # if none of the search modes succeeded then store the track id to the failure cache
    failure_cache.cache_match_failure(spotify_track['id'])


# def match_tracks_to_album(
#     tidal_album: tidalapi.Album,
#     spotify_playlist_tracks: Sequence[t_spotify.SpotifyTrack],
#     spotify_album_tracks: Sequence[t_spotify.SpotifyTrack],
#     threshold=0.6
# ):
#     spotify_album_track_ids = [ t['id'] for t in spotify_album_tracks ]
#     spotify_album = spotify_playlist_tracks[0]['album']
#     if test_album_similarity(spotify_album, tidal_album, threshold) and spotify_album['total_tracks'] == tidal_album.num_tracks:
#         tidal_album_tracks = tidal_album.tracks(limit=tidal_album.num_tracks)
#         assert len(tidal_album_tracks) == tidal_album.num_tracks, f"Expected {tidal_album.num_tracks} tracks, got {len(tidal_album_tracks)} for \"{spotify_album['name']}\""
#         assert len(tidal_album_tracks) == len(spotify_album_tracks)
#         tidal_playlist_tracks = []

#         # match each track
#         for spotify_track in spotify_playlist_tracks:
#             track_idx = spotify_album_track_ids.index(spotify_track['id'])

#             # we can't assert index to track number because some albums have multiple discs
#             # track_number = track_idx + 1
#             # track_numbers = [f"{track['disc_number']}:{track['track_number']}" for track in spotify_album_tracks]
#             # assert \
#             #     track_number == spotify_album_tracks[track_idx]['track_number'], \
#             #     f"track_number mismatch: {track_number} != {spotify_album_tracks[track_idx]['track_number']} / {track_numbers}"

#             tidal_track = tidal_album_tracks[track_idx]

#             # we also can't assert track / disc between spotify & tidal because they number differently...
#             # spotify_album_track = spotify_album_tracks[track_idx]
#             # assert \
#             #     tidal_track.track_num == spotify_album_track['track_number'] and tidal_track.volume_num == spotify_album_track['disc_number'],\
#             #     (
#             #         f"Tidal volume {tidal_track.volume_num} / Spotify disc {spotify_album_track['disc_number']} "
#             #         f"Tidal track number {tidal_track.track_num} / Spotify track number {spotify_album_track['track_number']}"
#             #     )

#             # if not match(tidal_track, spotify_track):
#             #     return None
#             tidal_playlist_tracks.append(tidal_track)
#         return tidal_playlist_tracks
#     return None


# async def tidal_search_album(spotify_playlist_tracks: Sequence[t_spotify.SpotifyTrack], rate_limiter, tidal_session: tidalapi.Session, spotify_session: spotipy.Spotify) -> tidalapi.Track | None:
#     async def _search_for_album_with_tracks():
#         # search for album name and all album artists
#         # get the album tracks (for track number)
#         spotify_album = spotify_playlist_tracks[0]['album']
#         spotify_album_tracks = await fetch_all_from_spotify_in_chunks(
#             repeat_on_request_error,
#             spotify_session.album_tracks,
#             spotify_album['id'],
#             is_playlist=False,
#         )
#         assert spotify_album['total_tracks'] == len(spotify_album_tracks)
#         artists = [ a['name'] for a in spotify_album['artists'] ]
#         query = " ".join([
#             simple(q)
#             for q in [ spotify_album['name'], *artists ]
#         ])
#         album_result = tidal_session.search(query, models=[tidalapi.album.Album])
#         for tidal_album in album_result['albums']:
#             if tidal_tracks := match_tracks_to_album(tidal_album, spotify_playlist_tracks, spotify_album_tracks):
#                 for spotify_track in spotify_playlist_tracks:
#                     failure_cache.remove_match_failure(spotify_track['id'])
#                 return tidal_tracks
#         # album_artist_names = '\n\t'.join([ f"{'/'.join(a.artists)} - {a.name}" for a in album_result['albums'] ])
#         # print(f"DID NOT FIND ALBUM: {query}\n{album_artist_names}")
#         return [ None for _ in spotify_playlist_tracks ]
    
#     def _search_for_standalone_track(spotify_track):
#         # if album search fails then search for track name and first artist
#         query = simple(spotify_track['name']) + ' ' + simple(spotify_track['artists'][0]['name'])
#         for track in tidal_session.search(query, models=[tidalapi.media.Track])['tracks']:
#             if match(track, spotify_track):
#                 failure_cache.remove_match_failure(spotify_track['id'])
#                 return track
#         return None

#     await rate_limiter.acquire()
#     album_search = await asyncio.to_thread( _search_for_album_with_tracks )
#     if album_search:
#         return album_search

#     tidal_tracks = []
#     for spotify_track in spotify_playlist_tracks:
#         await rate_limiter.acquire()
#         track_search = await asyncio.to_thread(_search_for_standalone_track, spotify_track)
#         tidal_tracks.append(track_search)
#         if not track_search:
#             # if none of the search modes succeeded then store the track id to the failure cache
#             failure_cache.cache_match_failure(spotify_track['id'])
#     return tidal_tracks


def populate_track_match_cache(spotify_tracks_: Sequence[t_spotify.SpotifyTrack], tidal_tracks_: Sequence[tidalapi.Track]):
    """ Populate the track match cache with all the existing tracks in Tidal playlist corresponding to Spotify playlist """
    def _populate_one_track_from_spotify(spotify_track: t_spotify.SpotifyTrack):
        for idx, tidal_track in list(enumerate(tidal_tracks)):
            if tidal_track.available and match(tidal_track, spotify_track):
                track_match_cache.insert((spotify_track['id'], tidal_track.id))
                tidal_tracks.pop(idx)
                return

    def _populate_one_track_from_tidal(tidal_track: tidalapi.Track):
        for idx, spotify_track in list(enumerate(spotify_tracks)):
            if tidal_track.available and match(tidal_track, spotify_track):
                track_match_cache.insert((spotify_track['id'], tidal_track.id))
                spotify_tracks.pop(idx)
                return

    # make a copy of the tracks to avoid modifying original arrays
    spotify_tracks = [t for t in spotify_tracks_]
    tidal_tracks = [t for t in tidal_tracks_]

    # first populate from the tidal tracks
    for track in tidal_tracks:
        _populate_one_track_from_tidal(track)
    # then populate from the subset of Spotify tracks that didn't match (to account for many-to-one style mappings)
    for track in spotify_tracks:
        _populate_one_track_from_spotify(track)


def get_new_spotify_tracks(spotify_tracks: Sequence[t_spotify.SpotifyTrack]) -> List[t_spotify.SpotifyTrack]:
    ''' Extracts only the tracks that have not already been seen in our Tidal caches '''
    results = []
    for spotify_track in spotify_tracks:
        if not spotify_track['id']: continue
        if not track_match_cache.get(spotify_track['id']) and not failure_cache.has_match_failure(spotify_track['id']):
            results.append(spotify_track)
    return results


def get_tracks_for_new_tidal_playlist(spotify_tracks: Sequence[t_spotify.SpotifyTrack]) -> Sequence[int]:
    ''' gets list of corresponding tidal track ids for each spotify track, ignoring duplicates '''
    output = []
    seen_tracks = set()

    for spotify_track in spotify_tracks:
        if not spotify_track['id']: continue
        tidal_id = track_match_cache.get(spotify_track['id'])
        if tidal_id:
            if tidal_id in seen_tracks:
                track_name = spotify_track['name']
                artist_names = ', '.join([artist['name'] for artist in spotify_track['artists']])
                print(f'Duplicate found: Track "{track_name}" by {artist_names} will be ignored') 
            else:
                output.append(tidal_id)
                seen_tracks.add(tidal_id)
    return output


@with_rate_limiter
async def search_new_tracks_on_tidal(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    spotify_tracks: Sequence[t_spotify.SpotifyTrack],
    playlist_name: str,
    config: dict,
    semaphore: asyncio.Semaphore,
):
    """ Generic function for searching for each item in a list of Spotify tracks which have not already been seen and adding them to the cache """
    # Extract the new tracks that do not already exist in the old tidal tracklist
    tracks_to_search = get_new_spotify_tracks(spotify_tracks)
    if not tracks_to_search:
        return

    # Get just the albums to search for
    # def _add_album_to_artist(accumulator: dict, track: t_spotify.SpotifyTrack):
    #     album_artists = ' '.join([
    #         a['name']
    #         for a in track['album']['artists']
    #     ])
    #     album_name = track['album']['name']
    #     if album_artists not in accumulator:
    #         accumulator[album_artists] = {}
    #     if album_name not in accumulator[album_artists]:
    #         accumulator[album_artists][album_name] = []
    #     accumulator[album_artists][album_name].append(track)
    #     return accumulator
    # artist_albums = reduce(_add_album_to_artist, tracks_to_search, {})

    # Search for each of the albums on Tidal concurrently
    task_description = f"Searching Tidal for {len(tracks_to_search)}/{len(spotify_tracks)} tracks in Spotify playlist '{playlist_name}'"
    search_results = await atqdm.gather(
        *[ tidal_search(t, semaphore, tidal_session) for t in tracks_to_search ],
        desc=task_description,
    )
    # search_results = await atqdm.gather(*[
    #     repeat_on_request_error(tidal_search_album, album_tracks, semaphore, tidal_session, spotify_session)
    #     for album in artist_albums.values() for album_tracks in album.values()
    # ], desc=task_description)
    # search_results = [ tidal_track for tidal_tracks in search_results for tidal_track in tidal_tracks ]

    # Add the search results to the cache
    song404 = []
    for idx, spotify_track in enumerate(tracks_to_search):
        if search_results[idx]:
            track_match_cache.insert( (spotify_track['id'], search_results[idx].id) )
        else:
            song404.append(f"{spotify_track['id']}: {','.join([a['name'] for a in spotify_track['artists']])} - {spotify_track['name']}")
            color = ('\033[91m', '\033[0m')
            print(color[0] + "Could not find the track " + song404[-1] + color[1])
    file_name = "songs not found.txt"
    with open(file_name, "a", encoding="utf-8") as file:
        for song in song404:
            file.write(f"{song}\n")


async def sync_playlist(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, spotify_playlist: dict, tidal_playlist: tidalapi.Playlist | None, config: dict):
    """ sync given playlist to tidal """
    # Get the tracks from both Spotify and Tidal, creating a new Tidal playlist if necessary
    spotify_tracks = await get_tracks_from_spotify_playlist(spotify_session, spotify_playlist)
    if len(spotify_tracks) == 0:
        return # nothing to do
    if tidal_playlist:
        old_tidal_tracks = await get_all_playlist_tracks(tidal_playlist)
    else:
        print(f"No playlist found on Tidal corresponding to Spotify playlist: '{spotify_playlist['name']}', creating new playlist")
        tidal_playlist = tidal_session.user.create_playlist(spotify_playlist['name'], spotify_playlist['description'])
        old_tidal_tracks = []

    # Extract the new tracks from the playlist that we haven't already seen before
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(tidal_session, spotify_session, spotify_tracks, spotify_playlist['name'], config)
    new_tidal_track_ids = get_tracks_for_new_tidal_playlist(spotify_tracks)
    print(f"Tidal matches: {len(new_tidal_track_ids)}")

    # Update the Tidal playlist if there are changes
    old_tidal_track_ids = [t.id for t in old_tidal_tracks]
    if new_tidal_track_ids == old_tidal_track_ids:
        print("No changes to write to Tidal playlist")
    elif new_tidal_track_ids[:len(old_tidal_track_ids)] == old_tidal_track_ids:
        # Append new tracks to the existing playlist if possible
        add_multiple_tracks_to_playlist(tidal_playlist, new_tidal_track_ids[len(old_tidal_track_ids):])
    else:
        # Erase old playlist and add new tracks from scratch if any reordering occured
        clear_tidal_playlist(tidal_playlist)
        add_multiple_tracks_to_playlist(tidal_playlist, new_tidal_track_ids)


async def sync_favorites(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config: dict):
    """ sync user favorites to tidal """
    async def get_tracks_from_spotify_favorites() -> List[dict]:
        _get_favorite_tracks = lambda offset=0: spotify_session.current_user_saved_tracks(offset=offset)    
        tracks = await fetch_all_from_spotify_in_chunks(repeat_on_request_error, _get_favorite_tracks)
        tracks.reverse()
        return tracks

    def get_new_tidal_favorites() -> List[int]:
        existing_favorite_ids = set([track.id for track in old_tidal_tracks])
        new_ids = []
        for spotify_track in spotify_tracks:
            match_id = track_match_cache.get(spotify_track['id'])
            if match_id and not match_id in existing_favorite_ids:
                new_ids.append(match_id)
        return new_ids

    print("Loading favorite tracks from Spotify")
    spotify_tracks = await get_tracks_from_spotify_favorites()
    print("Loading existing favorite tracks from Tidal")
    old_tidal_tracks = await get_all_favorites(tidal_session.user.favorites, order='DATE')
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(tidal_session, spotify_session, spotify_tracks, "Favorites", config)
    new_tidal_favorite_ids = get_new_tidal_favorites()
    if new_tidal_favorite_ids:
        for tidal_id in tqdm(new_tidal_favorite_ids, desc="Adding new tracks to Tidal favorites"):
            tidal_session.user.favorites.add_track(tidal_id)
    else:
        print("No new tracks to add to Tidal favorites")


def sync_playlists_wrapper(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, playlists, config: dict):
  for spotify_playlist, tidal_playlist in playlists:
    # sync the spotify playlist to tidal
    asyncio.run(sync_playlist(spotify_session, tidal_session, spotify_playlist, tidal_playlist, config) )


def sync_favorites_wrapper(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config):
    asyncio.run(main=sync_favorites(spotify_session=spotify_session, tidal_session=tidal_session, config=config))


def get_tidal_playlists_wrapper(tidal_session: tidalapi.Session) -> Mapping[str, tidalapi.Playlist]:
    tidal_playlists = asyncio.run(get_all_playlists(tidal_session.user))
    return {playlist.name: playlist for playlist in tidal_playlists}


def pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists: Mapping[str, tidalapi.Playlist]):
    if spotify_playlist['name'] in tidal_playlists:
      # if there's an existing tidal playlist with the name of the current playlist then use that
      tidal_playlist = tidal_playlists[spotify_playlist['name']]
      return (spotify_playlist, tidal_playlist)
    else:
      return (spotify_playlist, None)


def get_user_playlist_mappings(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config):
    results = []
    spotify_playlists = asyncio.run(get_playlists_from_spotify(spotify_session, config))
    tidal_playlists = get_tidal_playlists_wrapper(tidal_session)
    for spotify_playlist in spotify_playlists:
        results.append( pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists) )
    return results


def sync_playlists(args, spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config):
    if args.uri:
        # if a playlist ID is explicitly provided as a command line argument then use that
        spotify_playlist = spotify_session.playlist(args.uri)
        tidal_playlists = get_tidal_playlists_wrapper(tidal_session)
        tidal_playlist = pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists)
        sync_playlists_wrapper(spotify_session, tidal_session, [tidal_playlist], config)
        sync_favorites = args.sync_favorites # only sync favorites if command line argument explicitly passed
    elif args.sync_favorites:
        sync_favorites = True # sync only the favorites
    elif config.get('sync_playlists', None):
        # if the config contains a sync_playlists list of mappings then use that
        sync_playlists_wrapper(spotify_session, tidal_session, get_playlists_from_config(spotify_session, tidal_session, config), config)
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)
    else:
        # otherwise sync all the user playlists in the Spotify account and favorites unless explicitly disabled
        sync_playlists_wrapper(spotify_session, tidal_session, get_user_playlist_mappings(spotify_session, tidal_session, config), config)
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)

    if sync_favorites:
        sync_favorites_wrapper(spotify_session, tidal_session, config)

async def get_tracks_from_spotify_playlist(spotify_session: spotipy.Spotify, spotify_playlist):
    print(f"Loading tracks from Spotify playlist '{spotify_playlist['name']}'")
    fields = "next,total,limit,items(track(name,album(id,name,artists,total_tracks),artists,track_number,duration_ms,id,external_ids(isrc))),type"
    items = await fetch_all_from_spotify_in_chunks(
        repeat_on_request_error,
        spotify_session.playlist_tracks,
        playlist_id=spotify_playlist["id"],
        fields=fields,
    )
    track_filter = lambda item: item.get('type', 'track') == 'track' # type may be 'episode' also
    sanity_filter = lambda item: ('album' in item
                                  and 'name' in item['album']
                                  and 'artists' in item['album']
                                  and len(item['album']['artists']) > 0
                                  and item['album']['artists'][0]['name'] is not None)
    return list(filter(sanity_filter, filter(track_filter, items)))


async def get_playlists_from_spotify(spotify_session: spotipy.Spotify, config):
    # get all the playlists from the Spotify account
    playlists = []
    print("Loading Spotify playlists")
    first_results = spotify_session.current_user_playlists()
    exclude_list = set([x.split(':')[-1] for x in config.get('excluded_playlists', [])])
    playlists.extend([p for p in first_results['items']])
    user_id = spotify_session.current_user()['id']

    # get all the remaining playlists in parallel
    if first_results['next']:
        offsets = [ first_results['limit'] * n for n in range(1, math.ceil(first_results['total']/first_results['limit'])) ]
        extra_results = await atqdm.gather( *[asyncio.to_thread(spotify_session.current_user_playlists, offset=offset) for offset in offsets ] )
        for extra_result in extra_results:
            playlists.extend([p for p in extra_result['items']])

    # filter out playlists that don't belong to us or are on the exclude list
    my_playlist_filter = lambda p: p and p['owner']['id'] == user_id
    exclude_filter = lambda p: not p['id'] in exclude_list
    return list(filter( exclude_filter, filter( my_playlist_filter, playlists )))


def get_playlists_from_config(spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config):
    # get the list of playlist sync mappings from the configuration file
    def get_playlist_ids(config):
        return [(item['spotify_id'], item['tidal_id']) for item in config['sync_playlists']]
    output = []
    for spotify_id, tidal_id in get_playlist_ids(config=config):
        try:
            spotify_playlist = spotify_session.playlist(playlist_id=spotify_id)
        except spotipy.SpotifyException as e:
            print(f"Error getting Spotify playlist {spotify_id}")
            raise e
        try:
            tidal_playlist = tidal_session.playlist(playlist_id=tidal_id)
        except Exception as e:
            print(f"Error getting Tidal playlist {tidal_id}")
            raise e
        output.append((spotify_playlist, tidal_playlist))
    return output
