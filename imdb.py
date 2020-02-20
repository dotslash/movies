from __future__ import annotations

import csv
import os
import typing
from pathlib import Path

import attr
import requests


class Constants:
    IMDB_BASICS_TSV = Path.expanduser(Path('~/data/imdb/title.basics.tsv'))
    IMDB_AKAS_TSV = Path.expanduser(Path('~/data/imdb/title.akas.tsv'))
    OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '??')


@attr.s(auto_attribs=True)
class ImdbMovieInfo(object):
    titles: typing.Set[str] = attr.Factory(set)
    imdb_id: str = ''
    year: int = -1
    language: str = ''

    def enhance_data_using_api(self) -> ImdbMovieInfo:
        if not self.imdb_id:
            return self
        data = requests.get(f'https://www.omdbapi.com/?i={self.imdb_id}&apikey={Constants.OMDB_API_KEY}')
        self.language = data.json().get('Language', 'NA')
        return self


class ImdbMovieSet:
    def __init__(self):
        id_to_movie: typing.Dict[str, ImdbMovieInfo] = ImdbMovieSet._get_imdb_titles()
        name_to_id: typing.Dict[str, typing.Set[str]] = dict()
        for imdb_id, movie in id_to_movie.items():
            for title in movie.titles:
                name_to_id[ImdbMovieSet._normalize_name(title)].add(imdb_id)
        self.name_to_id = name_to_id
        self.id_to_movie = id_to_movie

    def lookup_movie(self, name):
        return [self.id_to_movie[imdb_id] for imdb_id in self.name_to_id.get(name, [])]

    @staticmethod
    def _get_imdb_titles() -> typing.Dict[str, ImdbMovieInfo]:
        ret = {}
        with Constants.IMDB_BASICS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                imdb_id, year, title_type = row['tconst'], row['startYear'], row['titleType']
                if title_type == 'movie':
                    titles = {row['primaryTitle'], row['originalTitle']}
                    ret[imdb_id] = ImdbMovieInfo(imdb_id=imdb_id, titles=titles, year=year)
        with Constants.IMDB_AKAS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                imdb_id, title = row['titleId'], row['title']
                movie = ret.get(imdb_id)
                if movie:
                    movie.titles.add(title)
        return ret

    @staticmethod
    def _normalize_name(inp):
        inp = inp.lower()
        import re
        inp = re.sub(r'[\W_]+', ' ', inp)
        inp = re.sub(r' +', ' ', inp)
        return inp.strip()
