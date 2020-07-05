from __future__ import annotations

import argparse
import csv
import json
import pprint
import re
import sqlite3
import sys
import threading
import typing
from collections import defaultdict
from pathlib import Path

import attr
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

IMDB_BASICS_TSV = Path.expanduser(Path('~/data/imdb/title.basics.tsv'))
IMDB_AKAS_TSV = Path.expanduser(Path('~/data/imdb/title.akas.tsv'))
MOVIES_DB = Path.expanduser(Path('~/data/movies.db'))
IS_DUMMY = False  # updated in __main__
AKA_SRC = "https://datasets.imdbws.com/title.akas.tsv.gz"
BASICS_SRC = "https://datasets.imdbws.com/title.basics.tsv.gz"


def log(msg):
    import time  # Adding the import to make this method easily copy+paste-able.
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{current_time} --> {msg}")


def is_empty_collection(inp):
    return inp is not None and len(inp) == 0


class ImdbSqliteHelper:
    CREATE_TABLES = '''
        CREATE TABLE IF NOT EXISTS "imdb" ( 
            "imdb_id" TEXT,
            "data" JSON,
            PRIMARY KEY("imdb_id")
        );
        CREATE TABLE IF NOT EXISTS "imdb_lookup" (
            "imdb_id" TEXT,
            "lookup_key" TEXT,
            "key_type" TEXT
        )
        CREATE UNIQUE INDEX IF NOT EXISTS "imdb_lookup_index" ON "imdb_lookup" (
            "lookup_key",
            "key_type",
            "imdb_id"
        )
    '''
    UPSERT_DATA_RECORD = """INSERT OR REPLACE INTO imdb (imdb_id,data) VALUES(?, ?);"""
    UPSERT_LOOKUP_RECORD = """
    INSERT OR REPLACE INTO imdb_lookup (imdb_id, lookup_key, key_type) VALUES(?, ?, ?);"""
    MOVIE_NAME_TO_MOVIES = """
        select data
        from imdb
        where imdb_id in (
            select imdb_id
            from imdb_lookup 
            where lookup_key = ? 
                   and key_type = 'norm_title')"""
    MOVIE_ID_TO_MOVIE = """
        select data
        from imdb
        where imdb_id = ?"""

    @staticmethod
    def insert_movie_queries(movie: ImdbMovieInfo):
        imdb_id = movie.imdb_id
        data = json.dumps(attr.asdict(movie))
        # if not movie.enhanced:
        # log(data)
        ret = [(ImdbSqliteHelper.UPSERT_DATA_RECORD, (imdb_id, data))]
        for region in movie.regions:
            ret.append((ImdbSqliteHelper.UPSERT_LOOKUP_RECORD, (imdb_id, region, "region")))
        for lang in movie.languages:
            ret.append((ImdbSqliteHelper.UPSERT_LOOKUP_RECORD, (imdb_id, lang, "lang")))
        norm_titles = {normalize_movie_name(title) for title in movie.titles}
        norm_titles = {norm_title for norm_title in norm_titles if norm_title}
        for norm_title in norm_titles:
            ret.append((ImdbSqliteHelper.UPSERT_LOOKUP_RECORD,
                        (imdb_id, normalize_movie_name(norm_title), "norm_title")))
        return ret


@attr.s(auto_attribs=True)
class ImdbMovieInfo(object):
    titles: typing.Set[str] = attr.Factory(set)
    imdb_id: str = attr.ib(default='')
    year: int = attr.ib(default=-1)
    enhanced: bool = attr.ib(default=False)
    enhancement_error: bool = attr.ib(default=False)
    languages: typing.Set[str] = attr.Factory(set)
    regions: typing.Set[str] = attr.Factory(set)
    detail: typing.Dict[str, typing.Set[str]] = attr.Factory(dict)

    def ensure_types(self):
        def _to_set(may_be_set):
            if type(may_be_set) != set:
                return set(may_be_set)

        self.languages = _to_set(self.languages)
        self.regions = _to_set(self.regions)
        self.titles = _to_set(self.titles)
        return self

    def _get_title_from_imdb_dot_com(self) -> typing.Dict[str, typing.Set[str]]:
        # See data_samples/imdb_title_titleDetails_div.html
        resp = requests.get(f'https://www.imdb.com/title/{self.imdb_id}')
        detail_entries = BeautifulSoup(resp.text, features="html.parser") \
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

    def enhance_data(self, error_func=None) -> ImdbMovieInfo:
        if not self.imdb_id:  # There is no imdb id.
            return self
        if self.enhanced or self.enhancement_error:  # Already enhanced.
            return self
        try:
            self.detail = self._get_title_from_imdb_dot_com()
            self.languages.update(self.detail.get('Language', set()))
            self.enhanced = True
            self.enhancement_error = False
        except Exception as e:
            self.enhancement_error = True
            self.enhanced = False
            error_func = error_func or log
            error_func(f"Failed enhancing : {self.imdb_id} {self.titles} {e}")
        return self


class ImdbMovieSet:
    def __init__(self, from_movie_names=None):
        if from_movie_names is not None:
            self.id_to_movie: typing.Dict[str, ImdbMovieInfo] = fetch_movies_from_name(
                from_movie_names)
        else:
            self.id_to_movie: typing.Dict[str, ImdbMovieInfo] = ImdbMovieSet._get_imdb_titles()

        self.name_to_id: typing.Dict[str, typing.Set[str]] = defaultdict(set)
        for imdb_id, movie in self.id_to_movie.items():
            for title in movie.titles:
                self.name_to_id[normalize_movie_name(title)].add(imdb_id)

    def lookup_movie(self, name) -> typing.List[ImdbMovieInfo]:
        name = normalize_movie_name(name)
        return [self.id_to_movie[imdb_id] for imdb_id in self.name_to_id.get(name, [])]

    @staticmethod
    def _get_imdb_titles() -> typing.Dict[str, ImdbMovieInfo]:
        ret = {}
        log(f"loading {IMDB_BASICS_TSV}")
        progress_basics = tqdm(desc="Basics progress")
        with IMDB_BASICS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                progress_basics.update(1)
                imdb_id, year, title_type = row['tconst'], row['startYear'], row['titleType']
                if IS_DUMMY and imdb_id > "tt0001000":
                    log(imdb_id)
                    break
                if title_type == 'movie':
                    titles = {row['primaryTitle'], row['originalTitle']}
                    ret[imdb_id] = ImdbMovieInfo(imdb_id=imdb_id, titles=titles, year=year)
        log(f"loaded {IMDB_BASICS_TSV}")
        log(f"loading {IMDB_AKAS_TSV}")
        progress_akas = tqdm(desc="AKA progress")
        with IMDB_AKAS_TSV.open() as tsvfile:
            reader = csv.DictReader(tsvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                progress_akas.update(1)
                imdb_id, title = row['titleId'], row['title']
                region, language = row['region'], row['language']
                movie = ret.get(imdb_id)
                if IS_DUMMY and imdb_id > "tt0001000":
                    log(imdb_id)
                    break
                if movie:
                    movie.titles.add(title)
                    if language != '\\N':
                        movie.languages.add(language)
                    if region != '\\N':
                        movie.regions.add(region)
        log(f"loaded {IMDB_AKAS_TSV}")
        if IS_DUMMY:
            log(ret)
        return ret

    def enhance_movie_info(self, imdb_ids=None) -> typing.List[str]:
        if is_empty_collection(imdb_ids):
            return []
        imdb_ids = imdb_ids or self.id_to_movie.keys()
        progress_bar = tqdm(total=len(imdb_ids))
        # error_bar = tqdm(desc="Errors")
        ret = []

        pool_sem = threading.BoundedSemaphore(value=5)

        def update_one_movie_and_update_progress(movie: ImdbMovieInfo):
            enhanced_before, enhancement_error_before = movie.enhanced, movie.enhancement_error
            try:  # attempt only if it's an error.
                movie.enhance_data(error_func=lambda x: tqdm.write(x))
            finally:
                pool_sem.release()
            lock = threading.Lock()
            with lock:
                if (movie.enhanced, movie.enhancement_error) != (enhanced_before, enhancement_error_before):
                    ret.append(movie.imdb_id)
                progress_bar.update()
            if IS_DUMMY: log(self.id_to_movie[movie.imdb_id])

        tasks = []
        for imdb_id in list(imdb_ids):
            movie = self.id_to_movie[imdb_id]
            pool_sem.acquire()
            t = threading.Thread(target=update_one_movie_and_update_progress, args=(movie,))
            t.start()
            tasks.append(t)
        for t in tasks:
            t.join()
        progress_bar.close()
        return ret

    def write_to_sqlite(self, imdb_ids=None):
        if is_empty_collection(imdb_ids):
            return
        conn = sqlite3.connect(str(MOVIES_DB))
        c = conn.cursor()
        num_movies_to_commit = 0
        total_count = 0
        imdb_ids = imdb_ids or self.id_to_movie.keys()
        log(f"Writing ImdbMovieSet to {MOVIES_DB}. num movies: {len(imdb_ids)}")
        status = tqdm(total=len(imdb_ids))
        for imdb_id in imdb_ids:
            movie = self.id_to_movie[imdb_id]
            queries = ImdbSqliteHelper.insert_movie_queries(movie)
            for q in queries:
                c.execute(q[0], q[1])
            num_movies_to_commit += 1
            total_count += 1
            if num_movies_to_commit >= 5000:
                conn.commit()
                status.update(num_movies_to_commit)
                num_movies_to_commit = 0
        if num_movies_to_commit > 0:
            status.update(num_movies_to_commit)
            conn.commit()
        conn.close()
        status.close()
        log(f"Wrote ImdbMovieSet to {MOVIES_DB}. num movies: {len(imdb_ids)}")


def fetch_movies_from_id(imdb_id: str) -> typing.Optional[ImdbMovieInfo]:
    conn = sqlite3.connect(str(MOVIES_DB))
    c = conn.cursor()
    c.execute(ImdbSqliteHelper.MOVIE_ID_TO_MOVIE, (imdb_id,))
    row = c.fetchone()
    if not row:
        return None
    return ImdbMovieInfo(**json.loads(row[0])).ensure_types()


def fetch_movies_from_name(names: typing.List[str]) -> typing.Dict[str, ImdbMovieInfo]:
    conn = sqlite3.connect(str(MOVIES_DB))
    c = conn.cursor()
    ret = {}
    norm_names = {normalize_movie_name(name) for name in names}
    status = tqdm(total=len(norm_names))
    for name in norm_names:
        c.execute(ImdbSqliteHelper.MOVIE_NAME_TO_MOVIES, (name,))
        for row in c.fetchall():
            movie_info = ImdbMovieInfo(**json.loads(row[0])).ensure_types()
            ret[movie_info.imdb_id] = movie_info
        status.update()
    status.close()
    return ret


# TODO: Move to a common util.
def normalize_movie_name(inp):
    inp = inp.lower()
    inp = re.sub(r'[\W_]+', ' ', inp)
    inp = re.sub(r' +', ' ', inp)
    return inp.strip()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--update', help='Reads IMDB csv files and updates sqlite', action='store_true')
    parser.add_argument('--lookup_id')
    parser.add_argument('--lookup_name')
    args = parser.parse_args()

    IS_DUMMY = args.debug

    if args.update:
        movie_set = ImdbMovieSet()
        log("Created ImdbMovieSet")
        if IS_DUMMY:
            enhanced = movie_set.enhance_movie_info(['tt0000009', 'tt0000675'])
            movie_set.write_to_sqlite(['tt0000009', 'tt0000675'])
        else:
            movie_set.write_to_sqlite()
    elif args.lookup_name:
        movies = ImdbMovieSet(from_movie_names=[args.lookup_name])
        print(json.dumps(list(attr.asdict(x) for x in movies.id_to_movie.values()), indent=2))
    elif args.lookup_id:
        movie = fetch_movies_from_id(args.lookup_id)
        if not movie:
            print("Movie not found")
        else:
            print(json.dumps(attr.asdict(movie), indent=2))
    else:
        parser.print_help()
        exit(-1)
