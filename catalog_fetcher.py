from __future__ import annotations

import argparse
import json
import typing

import attr
import requests
import tabulate
from bs4 import BeautifulSoup
from pathlib import Path
import tqdm

from imdb import ImdbMovieSet, ImdbMovieInfo, normalize_movie_name

FIREFOX_USER_AGENT = \
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
AMAZON_PRIME = "amazon_prime"
NETFLIX = "netflix"
CACHE_DIR = Path('~/data/catalog_fetcher_cache').expanduser()


def log(msg):
    import time  # Adding the import to make this method easily copy+paste-able.
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{current_time} --> {msg}")


def warn_if_false(inp, warning):
    if not inp:
        log(warning)


@attr.s(auto_attribs=True)
class PlatformId(object):
    platform: str = ""
    value: str = ""


def tryint(inp, default):
    try:
        return int(inp)
    except Exception:
        return default


@attr.s(auto_attribs=True)
class MovieInfo(object):
    name: str = ""
    languages: typing.Set[str] = attr.Factory(set)
    regions: typing.Set[str] = attr.Factory(set)
    imdb: typing.List[ImdbMovieInfo] = attr.Factory(list)
    platforms: typing.List[PlatformId] = attr.Factory(list)
    release_yr: int = -1
    src_to_raw_entry: typing.Dict[str, typing.Dict[str, str]] = attr.Factory(dict)

    def get_netflix_url(self):
        for p in self.platforms:
            if p.platform == NETFLIX and not p.value.startswith('slug'):
                return f'https://www.netflix.com/title/{p.value}'
        for v in self.src_to_raw_entry.values():
            netflixid = v.get('netflixid')
            if netflixid:
                return f'https://www.netflix.com/title/{netflixid}'
        return None

    def get_imdb_rating(self):
        for v in self.src_to_raw_entry.values():
            imdb_rating = v.get('imdb')
            imdb_rating = imdb_rating or v.get('imdb_rating')
            if imdb_rating:
                return imdb_rating
        return 'n/a'

    def update_imdb(self, imdb_movie_set: ImdbMovieSet) -> MovieInfo:
        self.imdb = imdb_movie_set.lookup_movie(self.name)
        for imdb_movie_info in self.imdb:
            self.languages.update(imdb_movie_info.languages)
            self.regions.update(imdb_movie_info.regions)
        return self

    def to_trimmed_str(self):
        return f'''-->Name: {self.name} <-- Languages: {','.join(
            self.languages)} Year: {self.release_yr} ''' + \
               f'''Sources: {','.join(self.src_to_raw_entry.keys())} ''' + \
               f'''ImdbInfo: {",".join([x.imdb_id for x in self.imdb])} '''

    def is_equivalent(self, other: MovieInfo):
        self_imdbs = set(i.imdb_id for i in self.imdb)
        other_imdbs = set(i.imdb_id for i in other.imdb)
        if self_imdbs == other_imdbs and len(self_imdbs) == 0:
            return True
        other_platforms = {str(p) for p in other.platforms}
        self_platforms = {str(p) for p in self.platforms}
        if not other_platforms.isdisjoint(self_platforms):
            return True
        return normalize_movie_name(self.name) == normalize_movie_name(other.name) and \
               self.release_yr == other.release_yr

    def matches(self, languages=None, release_yr=None) -> bool:
        assert languages or release_yr is not None
        languages = languages or set()
        lang_check = not languages or not languages.isdisjoint(self.languages)
        release_yr_check = not release_yr or \
                           release_yr == self.release_yr or \
                           (self.release_yr == -1 and release_yr in {m.year for m in self.imdb})
        return lang_check and release_yr_check

    @staticmethod
    def from_whats_on_netflix(entry: typing.Dict) -> MovieInfo:
        """
        {
            "title": "​Mayurakshi",
            "type": "Movie",
            "titlereleased": "2017",
            "image_landscape": "https://occ-0-114-116.1.nflxso.net/art/8368f/...",
            "image_portrait": "http://occ-0-2430-116.1.nflxso.net/dnm/api/v6/...",
            "rating": "TV-14",
            "quality": "SuperHD",
            "actors": "Soumitra Chatterjee, Prasenjit Chatterjee, Indrani Haldar, ...",
            "director": "Atanu Ghosh",
            "category": "Dramas\n                  International Movies",
            "imdb": "7.1/10",
            "runtime": "99 minutes",
            "netflixid": "81018236",
            "date_released": "2018-09-15",
            "description": "When a middle-aged...."
        }
        """
        return MovieInfo(
            name=entry['title'], src_to_raw_entry={"whats-on-netflix.com": entry},
            release_yr=entry["titlereleased"],
            platforms=[PlatformId(platform=NETFLIX, value=entry['netflixid'])]
        )

    @staticmethod
    def _finder_entry_to_dict(bs_entry):
        tds = bs_entry.find_all("td")
        # Each row has the following columns.
        """
        <td data-title="Title" scope="row"><b>#Roxy</b><span class="badges"></span></td>,
        <td data-title="Year of release" scope="row">2018</td>,
        <td data-title="Runtime (mins)" scope="row">105</td>,
        <td data-title="Genres" scope="row">Canadian Movies</td>,
        <td><a class="btn btn-success" href="http://www.netflix.com/watch/81087095" rel="nofollow">Watch now</a></td>
        """

        def td_to_kv(td):
            anchors = td.find_all("a")
            if len(anchors) == 0:
                return td.attrs["data-title"], td.text
            return "watch_link", anchors[0].attrs["href"]

        kvs = [td_to_kv(td) for td in tds]
        return {k: v for k, v in kvs}

    @staticmethod
    def from_finder(provider, bs_entry):
        data = MovieInfo._finder_entry_to_dict(bs_entry)
        if provider == NETFLIX:
            provider_id = data["watch_link"].strip("/").split("/")[-1]
        else:
            provider_id = f"NA_{data['Title']}"
        return MovieInfo(name=data["Title"], src_to_raw_entry={f"finder.com:{provider}": data},
                         release_yr=tryint(data["Year of release"], -1),
                         platforms=[PlatformId(platform=provider, value=provider_id)])


class MovieInfoCollection:
    def __init__(self, movie_infos: typing.List[MovieInfo]):
        self.unknown: typing.List[MovieInfo] = [mi for mi in movie_infos if len(mi.imdb) == 0]
        self.movie_by_imdb: typing.Dict[str, typing.List[MovieInfo]]
        for mi in movie_infos:
            for mi_imdb in mi.imdb:
                self.movie_by_imdb[mi_imdb.imdb_id].append(mi)

    def merge_with(self, other: MovieInfoCollection):
        pass


def fetch_from_whats_on_netflix(enhance) -> typing.List[MovieInfo]:
    # curl equivalent of the request made by browser.
    """
    curl 'https://www.whats-on-netflix.com/wp-content/plugins/whats-on-netflix/json/movie.json'
          -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
          -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
          -H 'Accept-Language: en-US,en;q=0.5'
          --compressed
          -H 'DNT: 1'
          -H 'Connection: keep-alive'
          -H 'Upgrade-Insecure-Requests: 1'
    """
    url = "https://www.whats-on-netflix.com/wp-content/plugins/whats-on-netflix/json/movie.json"
    headers = {'User-Agent': FIREFOX_USER_AGENT}
    resp = requests.get(url, headers=headers)
    log("Fetched data from WON")
    ret = [MovieInfo.from_whats_on_netflix(entry) for entry in resp.json()]
    imdb_set = ImdbMovieSet(from_movie_names=[m.name for m in ret])
    log("IMDBMovie set created")
    if enhance:
        enhanced = imdb_set.enhance_movie_info()
        log("Enhanced IMDBMovie set")
        imdb_set.write_to_sqlite(enhanced)
        log("Wrote Enhancements back to db")
    return [m.update_imdb(imdb_set) for m in ret]


def update_imdb_for_set():
    pass


def fetch_from_finder(provider, enhance):
    # curl equivalent of the request made by browser.
    """
    curl 'https://www.finder.com/netflix-movies'
         -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
         -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
         -H 'Accept-Language: en-US,en;q=0.5'
         --compressed
         -H 'DNT: 1'
         -H 'Connection: keep-alive'
         -H 'Cookie: _gcl_au=1.1.1399362633.1582008201; _sp_ses.5dec=*; _sp_id.5dec=67ec6f86-5126-4b62-9079-a553398f1b7e.1582008201.1.1582008276.1582008201.e951a179-d0b4-4b83-bd1e-db70e6980bad; __futm_op=0; __futm=google; __futm_session=; __futm_data=%7B%22utm_source%22%3A%22google%22%2C%22utm_medium%22%3A%22organic%22%2C%22utm_landing_page_id%22%3A792081%2C%22utm_landing_page_country%22%3A%22us%22%7D; geoip_checked_us=true'
         -H 'Upgrade-Insecure-Requests: 1'
         -H 'TE: Trailers'
    """
    url = {
        NETFLIX: "https://www.finder.com/netflix-movies",
        AMAZON_PRIME: "https://www.finder.com/amazon-prime-movies"
    }[provider]
    headers = {'User-Agent': FIREFOX_USER_AGENT}
    resp = requests.get(url, headers=headers)
    tables = BeautifulSoup(resp.text, "html.parser").find_all(
        "table", attrs={"class": "luna-table luna-table--responsiveList ts-table"})
    warn_if_false(len(tables) == 1, f"expected only 1 table, but found {len(tables)}")
    table = tables[0]
    rows = table.find_all("tr")
    row0tds = rows[0].find_all('td')
    warn_if_false(len(row0tds) == 0,
                  "Expecting rows[0] to be header, have no td entries.\n" +
                  f"rows0: {rows[0]}\n" +
                  f"rows1: {rows[1]}")

    ret = [MovieInfo.from_finder(provider, row) for row in rows[1:]]
    imdb_set = ImdbMovieSet(from_movie_names=[m.name for m in ret])
    if enhance:
        log("IMDBMovie set created")
        enhanced = imdb_set.enhance_movie_info()
        log("Enhanced IMDBMovie set")
        imdb_set.write_to_sqlite(enhanced)
        log("Wrote Enhancements back to db")
    return [m.update_imdb(imdb_set) for m in ret]


def get_reelgood_url(provider, start, count):
    amazon_format = 'https://api.reelgood.com/v3.0/content/browse/source/amazon?availability=onSources&' + \
                    'content_kind=movie&hide_seen=false&hide_tracked=false&hide_watchlisted=false&imdb_end=10&' + \
                    'imdb_start=0&override_user_sources=true&overriding_free=false&overriding_sources=amazon_prime&' + \
                    'region=us&rt_end=100&rt_start=0&{}&sort=0&sources=amazon_prime&{}&year_end=2020&year_start=1900'
    netflix_format = 'https://api.reelgood.com/v3.0/content/browse/source/netflix?availability=onSources&' + \
                     'content_kind=movie&hide_seen=false&hide_tracked=false&hide_watchlisted=false&imdb_end=10&' + \
                     'imdb_start=0&override_user_sources=true&overriding_free=false&overriding_sources=netflix&' \
                     'region=us&rt_end=100&rt_start=0&{}&sort=0&sources=netflix&{}&year_end=2020&year_start=1900'
    url_formats = {AMAZON_PRIME: amazon_format, NETFLIX: netflix_format}
    return url_formats[provider].format(f'skip={start}', f'take={count}')


def populate_reelgood_cache(provider):
    CACHE_DIR.mkdir(exist_ok=True)
    done_file = Path(str(CACHE_DIR) + f'/reelgood_{provider}_done')
    if done_file.exists():  # TODO: Recency
        return
    # Clear potentially corrupt reelgood files.
    old_files = CACHE_DIR.glob(f'reelgood_{provider}*')
    for of in old_files:
        of.unlink()
    # Fetch page by page.
    PAGE_SIZE = 200
    progress_bar = tqdm.tqdm(desc=f"reelgood for {provider}")
    for page in range(1000):
        url = get_reelgood_url(provider, PAGE_SIZE * page, PAGE_SIZE)
        resp = requests.get(url)
        of = Path(str(CACHE_DIR) + f'/reelgood_{provider}_from_{PAGE_SIZE * page}_sz_{PAGE_SIZE}.json')
        of.write_text(json.dumps(resp.json()))
        progress_bar.update(PAGE_SIZE)
        if len(resp.json()['results']) == 0:
            break
    # Set the done file.
    done_file.touch()


def fetch_from_reelgood(provider, enhance):
    print(f"Fetching {provider} from reelgood")
    populate_reelgood_cache(provider)
    files = CACHE_DIR.glob(f'reelgood_{provider}*.json')
    ret: typing.List[MovieInfo] = []
    for f in files:
        movies = json.loads(f.read_text())['results']
        for m in movies:
            ret.append(MovieInfo(
                name=m['title'],
                src_to_raw_entry={f"reelgood:{provider}": m},
                release_yr=tryint(m.get('released_on', '-1')[:4], -1),
                platforms=[PlatformId(platform=provider, value=f'slug_{m["slug"]}')]
            ))
    imdb_set = ImdbMovieSet(from_movie_names=[m.name for m in ret])
    if enhance:
        log("IMDBMovie set created")
        enhanced = imdb_set.enhance_movie_info()
        log("Enhanced IMDBMovie set")
        imdb_set.write_to_sqlite(enhanced)
        log("Wrote Enhancements back to db")
    return [m.update_imdb(imdb_set) for m in ret]


def merge_netflix(movies1: typing.List[MovieInfo], movies2: typing.List[MovieInfo]):
    movies_by_id: typing.Dict[str, MovieInfo] = {m.get_netflix_url(): m for m in movies1 if
                                                 m.get_netflix_url()}
    for m in movies2:
        nurl = m.get_netflix_url()
        if not nurl:
            assert False, m
        existing = movies_by_id.get(nurl)
        if not existing:
            movies_by_id[nurl] = m
            continue
        # print(m)
        existing.src_to_raw_entry.update(m.src_to_raw_entry)
    return list(movies_by_id.values())


def get_netflix_all(legacy, enhance):
    if legacy:
        movies1: typing.List[MovieInfo] = fetch_from_whats_on_netflix(enhance)
        movies2: typing.List[MovieInfo] = fetch_from_finder(NETFLIX, enhance)
        return merge_netflix(movies1, movies2)
    else:
        return fetch_from_reelgood(NETFLIX, enhance)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--provider', choices=['netflix', 'amazon'], default='netflix')
    parser.add_argument('--lang', help='Comma separated language filter', default='')
    parser.add_argument('--year', help='Release year filter', type=int)
    parser.add_argument('--enhance', help='Enhance netflix movie information', type=bool, default=False)
    parser.add_argument('--legacy', help='Use legacy fetcher', type=bool, default=False)
    parser.add_argument('--sortby', choices=['year', 'rating'], default='year')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.provider == 'netflix':
        movies = get_netflix_all(args.legacy, args.enhance)
    elif args.provider == 'amazon':
        if args.legacy:
            movies = fetch_from_finder(AMAZON_PRIME, args.enhance)
        else:
            movies = fetch_from_reelgood(AMAZON_PRIME, args.enhance)

    languages = set()
    if args.lang:
        languages = set(args.lang.split(','))
    filtered_movies: typing.List[MovieInfo] = [
        m for m in movies if m.matches(languages=languages, release_yr=args.year)]
    filtered_movies.sort(key=lambda x: -int(x.release_yr))
    rows = [[m.name, m.release_yr,
             m.get_imdb_rating(),
             ','.join(set(l for l in m.languages if len(l) != 2))[:20],
             ','.join(set(l for l in m.languages if len(l) == 2))[:20],
             ','.join([x.imdb_id for x in m.imdb])[:50]]
            for m in filtered_movies]
    SORTKEY_IND = 1  # year
    if args.sortby == 'rating':
        SORTKEY_IND = 2  # rating
    rows.sort(key=lambda x: -tryint(x[SORTKEY_IND], -1))
    print(tabulate.tabulate(
        rows,
        headers=["name", "year", "imdb_rating", "lang", "lang_alt", "imdb"]))


if __name__ == '__main__':
    main()
