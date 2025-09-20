import argparse
import sys

import yaml

from . import auth as _auth
from .follows import sync_artist_follows
from .playlists import sync_playlists


def main():
    parser = argparse.ArgumentParser(prog='spotify-to-tidal')
    parser.add_argument('--config', default='config.yml', help='location of the config file')
    subparsers = parser.add_subparsers(required=True)

    playlists = subparsers.add_parser('playlists', help='Sync playlists')
    playlists.add_argument('--uri', help='synchronize a specific URI instead of the one in the config')
    playlists.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction, help='synchronize the favorites')
    playlists.set_defaults(func=sync_playlists)

    artist_follows = subparsers.add_parser('artist-follows', help='Sync followed artists')
    artist_follows.set_defaults(func=sync_artist_follows)

    args = parser.parse_args()
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    print("Opening Spotify session")
    spotify_session = _auth.open_spotify_session(config['spotify'])
    print("Opening Tidal session")
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to Tidal")

    args.func(args, spotify_session, tidal_session, config)

if __name__ == '__main__':
    main()
