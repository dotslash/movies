from __future__ import annotations

import requests
import attr
import typing
from bs4 import BeautifulSoup

from imdb import ImdbMovieSet, ImdbMovieInfo, normalize_movie_name

FIREFOX_USER_AGENT = \
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
AMAZON_PRIME = "amazon_prime"
NETFLIX = "netflix"


def warn_if_false(inp, warning):
    if not inp:
        print(warning)


@attr.s(auto_attribs=True)
class PlatformId(object):
    platform: str = ""
    value: str = ""


@attr.s(auto_attribs=True)
class MovieInfo(object):
    name: str = ""
    language: str = ""
    imdb: typing.List[ImdbMovieInfo] = attr.Factory(list)
    platforms: typing.List[PlatformId] = attr.Factory(list)
    release_yr: int = -1
    src_to_raw_entry: typing.Dict[str, typing.Dict[str, str]] = attr.Factory(dict)

    def update_imdb(self, imdb_movie_set: ImdbMovieSet) -> MovieInfo:
        self.imdb = imdb_movie_set.lookup_movie(self.name)
        return self

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
                         release_yr=data["Year of release"],
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


def fetch_from_whats_on_netflix() -> typing.List[MovieInfo]:
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
    return [MovieInfo.from_whats_on_netflix(entry) for entry in resp.json()]


def fetch_from_finder(provider):
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
    warn_if_false(row0tds == 0,
                  f"expecting rows[0] to be the header and have no td entries. actual {row0tds}")
    return [MovieInfo.from_finder(provider, row) for row in rows[1:]]


if __name__ == '__main__':
    # Manual testing.
    res = fetch_from_whats_on_netflix()
    print(res[0])
    res = fetch_from_finder(NETFLIX)
    print(res[0])
    res = fetch_from_finder(AMAZON_PRIME)
    print(res[0])
