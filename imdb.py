from __future__ import annotations

import csv
import typing
from pathlib import Path

import attr
import requests
from bs4 import BeautifulSoup

IMDB_BASICS_TSV = Path.expanduser(Path('~/data/imdb/title.basics.tsv'))
IMDB_AKAS_TSV = Path.expanduser(Path('~/data/imdb/title.akas.tsv'))


@attr.s(auto_attribs=True)
class ImdbMovieInfo(object):
    titles: typing.Set[str] = attr.Factory(set)
    imdb_id: str = ''
    year: int = -1
    languages: typing.Set[str] = attr.Factory(set)
    detail: typing.Dict[str, typing.Set[str]] = attr.Factory(dict)

    def _get_title_from_imdb_dot_com(self) -> typing.Dict[str, typing.Set[str]]:
        # See data_samples/imdb_title_titleDetails_div.html
        resp = requests.get(f'https://www.imdb.com/title/{self.imdb_id}')
        detail_entries = BeautifulSoup(resp.text) \
            .find_all(id='titleDetails')[0] \
            .find_all("div", attrs={'class': 'txt-block'})
        ret = {}
        # The parsing here is not very accurate. It is not even best effort
        for detail_entry in detail_entries:
            inline_divs = detail_entry.find_all(attrs={"class": "inline"})
            if len(inline_divs) == 0:
                continue
            key = inline_divs[0].text.strip(":")
            val = {a.text for a in detail_entry.find_all("a")}
            is_see_more = len([v for v in val if v.strip() == 'See more']) != 0
            if is_see_more or (len(val)) == 0:
                val = {detail_entry.text.split(":")[1].split("See more")[0].strip()}
            ret[key] = val

        return ret

    def enhance_data(self) -> ImdbMovieInfo:
        if not self.imdb_id:
            return self
        self.detail = self._get_title_from_imdb_dot_com()
        self.languages = self.detail.get('Language', set())
        return self


class ImdbMovieSet:
    def __init__(self):
        self.id_to_movie: typing.Dict[str, ImdbMovieInfo] = ImdbMovieSet._get_imdb_titles()
        self.name_to_id: typing.Dict[str, typing.Set[str]] = dict()
        for imdb_id, movie in self.id_to_movie.items():
            for title in movie.titles:
                self.name_to_id[normalize_movie_name(title)].add(imdb_id)

    def lookup_movie(self, name) -> typing.List[ImdbMovieInfo]:
        name = normalize_movie_name(name)
        return [self.id_to_movie[imdb_id] for imdb_id in self.name_to_id.get(name, [])]

    @staticmethod
    def _get_imdb_titles() -> typing.Dict[str, ImdbMovieInfo]:
        ret = {}
        with IMDB_BASICS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                imdb_id, year, title_type = row['tconst'], row['startYear'], row['titleType']
                if title_type == 'movie':
                    titles = {row['primaryTitle'], row['originalTitle']}
                    ret[imdb_id] = ImdbMovieInfo(imdb_id=imdb_id, titles=titles, year=year)
        with IMDB_AKAS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                imdb_id, title = row['titleId'], row['title']
                movie = ret.get(imdb_id)
                if movie:
                    movie.titles.add(title)
        return ret


# TODO: Move to a common util.
def normalize_movie_name(inp):
    inp = inp.lower()
    import re
    inp = re.sub(r'[\W_]+', ' ', inp)
    inp = re.sub(r' +', ' ', inp)
    return inp.strip()
