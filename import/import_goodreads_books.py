# a Jupyter notebook to investigate the data before creating import script
import pandas as pd
import numpy as np
import json as json
from sqlalchemy import create_engine
from sqlalchemy.types import *

engine = create_engine(f'postgresql://neilmeehan:JulianFeb2017:(@localhost:5432/book_recs')

# set datatypes
dtypes = {
    'book_id': pd.Int32Dtype(),
    'work_id': pd.Int32Dtype(),
    'title': pd.StringDtype(),
    'title_without_series': pd.StringDtype(),
    'isbn': pd.Int64Dtype(),
    'isbn13': pd.Int64Dtype(),
    'text_reviews_count': pd.Int32Dtype(),
    'ratings_count': pd.Int32Dtype(),
    'average_rating': pd.Float32Dtype(),
    'series': pd.StringDtype(),
    'country_code': pd.StringDtype(),
    'language_code': pd.StringDtype(),
    'asin': pd.StringDtype(),
    'kindle_asin': pd.StringDtype(),
    'is_ebook': pd.StringDtype(),
    'description': pd.StringDtype(),
    'link': pd.StringDtype(),
    'url': pd.StringDtype(),
    'image_url': pd.StringDtype(),
    'num_pages': pd.Int32Dtype(),
    'publication_day': pd.Int8Dtype(),
    'publication_month': pd.Int8Dtype(),
    'publication_year': pd.Int16Dtype(),
    'format': pd.StringDtype(),
    'publisher': pd.StringDtype(),
    'edition_information': pd.StringDtype(),
    'similar_books': pd.StringDtype(),
    'authors': pd.StringDtype(),
    'popular_shelves': pd.StringDtype()
}

FILE_PATH = 'https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/goodreads/'
files = [
    'goodreads_books', 
    'goodreads_book_authors', 
    # 'goodreads_book_genres_initial',
    # 'goodreads_book_works'
        ]

chunked = pd.read_json(f'{FILE_PATH}{files[0]}.json.gz', 
                       lines=True, 
                       dtype=dtypes,
                       chunksize=10000)

print('Chunking done.')

# set the schema for the tables
BOOKS_SCHEMA = {
    'book_id': Integer,
    'work_id': Integer,
    'title': Text,
    'title_without_series': Text,
    'isbn': BigInteger,
    'isbn13': BigInteger,
    'text_reviews_count': Integer,
    'ratings_count': Integer,
    'average_rating': Float,
    'series': Text,
    'country_code': Text,
    'language_code': Text,
    'asin': Text,
    'kindle_asin': Text,
    'is_ebook': Text,
    'description': Text,
    'link': Text,
    'url': Text,
    'image_url': Text,
    'num_pages': Integer,
    'publication_day': Integer,
    'publication_month': Integer,
    'publication_year': Integer,
    'format': Text,
    'publisher': Text,
    'edition_information': Text,
    'similar_books': Text
}

AUTHORS_SCHEMA = {
    'author_id': Integer,
    'work_id': Integer,
    'role': Text
}

TAGS_SCHEMA = {
    'work_id': Integer,
    'name': Text,
    'count':  Integer
}

# set the columns which need to be converted
to_conv = ['book_id',
           'work_id',
           'text_reviews_count',
           'ratings_count',
           'isbn',
           'isbn13',
           'num_pages',
           'publication_day',
           'publication_month',
           'publication_year',
           'average_rating']

counter = 1
seen_works = set()

for piece in chunked:
    # filter to english books
    piece = piece[piece['language_code'].str.contains('en') | piece['language_code'] == '']
    
    # get tags df
    df_exploded = piece.explode('popular_shelves').reset_index(drop=True)
    df_normalized = pd.json_normalize(df_exploded['popular_shelves']).reset_index(drop=True)
    tags = df_exploded[['work_id']].join(df_normalized)
    # only add unseen works
    tags = tags[tags['work_id'].isin(seen_works)]
    for id in tags['work_id'].unique():
        seen_works.add(id)

    print(tags)
    tags[['work_id', 'count']] = tags[['work_id', 'count']].apply(pd.to_numeric, errors='coerce')
    tags.to_sql('tags',
                engine,
                if_exists='append',
                index=False,
                dtype=TAGS_SCHEMA)

    # get authors df
    df_exploded = piece.explode('authors').reset_index(drop=True)
    df_normalized = pd.json_normalize(df_exploded['authors']).reset_index(drop=True)
    authors = df_exploded[['work_id']].join(df_normalized)
    authors[['work_id', 'author_id']] = authors[['work_id', 'author_id']].apply(pd.to_numeric, errors='coerce')
    authors.to_sql('authors',
                   engine,
                   if_exists='append',
                   index=False,
                   dtype=AUTHORS_SCHEMA)

    # drop exploded columns
    goodreads_books = piece.drop(['popular_shelves', 'authors'], axis=1).astype(pd.StringDtype)
    goodreads_books[to_conv] = goodreads_books[to_conv].apply(pd.to_numeric, errors='coerce')
    goodreads_books.to_sql('goodreads_books',
                           engine,
                           if_exists='append',
                           index=False,
                           dtype =BOOKS_SCHEMA)
        
    print(F'FINISHED CHUNK {counter}')
    counter += 1

    # break