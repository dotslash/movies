# movies

I want this to be a repository of misc utils to discover movies. The following documentation is mostly for my own 
sanity when I poke these scripts in the future again.

Note that as of, the code has a lots of assumptions about being run on my laptop. If im satisfied with what I have 
here, I will work "easily" on making this work without these assumptions.

Current state of things 

- `imdb.py`: Expects that akas and basics datasets (https://datasets.imdbws.com) are present in `~/data`. It creates
   or updates a sqlite file at `~/data/movies.db` consolidating information from these 2 datasets.
- `catalog_fetcher`: Builds upon `~/data/movies.db` generated by `imdb.py` and allows filtering movies present in
   netflix, amazon prime. Currently language and release year filters are supported. The imdb dataset created by
   `imdb.py` is very basic. This script enhances the imdb information and updates `movies.db`. Example usage

```sh
$ python catalog_fetcher.py --provider amazon --lang te,Telugu,hi,Hindi --year 2019 --sortby rating
Fetching amazon_prime from reelgood
Fetching from movie names: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 13590/13590 [00:01<00:00, 10732.77it/s]
name                                     year    imdb_rating  lang                  lang_alt              imdb
-------------------------------------  ------  -------------  --------------------  --------------------  --------------------
Agent Sai Srinivasa Athreya              2019            8.5                        en,hi                 tt10214826
Jallikattu                               2019            7.7                        en,hi                 tt8721556
Driving Licence                          2019            7.2                        en,hi                 tt9264336
Good Newwz                               2019            7                          en,hi                 tt8504014
Mr. Toilet: The World's #2 Man           2019            7.1  Hindi,Telugu,English  en                    tt10263296
Dear Comrade                             2019            7.3                        hi                    tt8388508
Mehandi Circus                           2019            7.5  Tamil                 hi                    tt9665400
Knives Out                               2019            7.9  cmn,Spanish,yue,Hind  bg,he,tr,en,hi,ja,fr  tt8946378
Rocketman                                2019            7.3  English               bg,he,tr,en,hi,ja,fr  tt8372368,tt2066051
The Lighthouse                           2019            7.6  cmn,English           tr,en,hi,ja,fr        tt7984734
Midsommar                                2019            7.1  Swedish,English       he,tr,en,hi,sr,ja,fr  tt8772262
The Last Black Man in San Francisco      2019            7.4  English               en,hi,ja              tt4353250
The Report                               2019            7.2  English               en,hi,ja              tt8236336
Low Tide                                 2019            6.2                        en,hi                 tt7434324
Rambo: Last Blood                        2019            6.2  cmn,English,Spanish   bg,he,tr,en,hi,ja,fr  tt1206885
Troop Zero                               2019            6.9  English               en,hi,fr              tt2404465
Crawl                                    2019            6.2  English               bg,he,tr,en,hi,ja,fr  tt8364368
The Aeronauts                            2019            6.6  French,Latin,English  he,tr,en,hi,ja        tt6141246
Late Night                               2019            6.5  English               tr,he,en,hi,ja,fr     tt6107548
Teacher                                  2019            6.5                        hi                    tt7281538
Bigil                                    2019            6.8                        ta,te,en,hi           tt9260636
Chhota Bheem Kung Fu Dhamaka             2019            6.5  Hindi,English         hi                    tt10288820
The Souvenir                             2019            6.5                        en,hi,ja              tt6920356
Santa Fake                               2019            5.4                        hi                    tt6201302
47 Meters Down: Uncaged                  2019            5    English               fr,en,hi,ja           tt7329656
The Kill Team                            2019            5.9                        tr,en,hi,fr           tt6196936
Escape and Evasion                       2019            5.1                        hi                    tt7423486
Kill Chain                               2019            5                          en,hi,ja              tt8535180
Bharat                                   2019            5.3                        en,hi                 tt7721800
The League of Legend Keepers: Shadows    2019            5.4                        hi                    tt6170432
Petromax                                 2019            5.5                        en,hi                 tt10987184
Bottom of the 9th                        2019            5.5                        en,hi                 tt1507002
...
```   