import pandas as pd
import numpy as np
import cutie
from functions import helpers
from sqlalchemy import create_engine
from secrets import user, password

engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/book_recs')

name = input('Please enter a name for your booklist: ')
print(f"\nAwesome! Let's start building out {name}.\n")

list = []

options = ['Search for a book.',
           'Search for an author.']
print("How would you like to begin?\n")
start = options[cutie.select(options)]

if start == options[0]:

    temp = input('\nPlease enter the name of the book you are searching for: ')

    df = helpers.search_title(engine, temp).head(10)
    options = df['title'].str.title() + ' by ' + df['author_name'].str.title()

    print('\n')

    selected = options[cutie.select(options)]
    list = df.iloc[cutie.select(options), 'work_id']

else:

    temp = input('\nPlease enter the name of the author you are searching for: ')

    df = helpers.search_author(engine, temp)
    options1 = df['author_name'].str.title()

    selected = options1[cutie.select(options1)]
    authorid = df.loc[df['author_name'] == selected.upper(), 'author_id'][0]

    print(f"\nGetting {selected}'s books...\n")

    df = helpers.get_author_books(engine, authorid)

    print('Title, Average Rating, Number of Ratings')
    # options2 = df_sub['title'].str.title()
    options2 = [f'{row['title'].title()}, {np.round(row['average_rating'], 2)}, {row['ratings_count']:,}' for _, row in df.head(10).iterrows()]
    
    selected = cutie.select_multiple(options2, minimal_count=1)

    list = df.iloc[selected]['work_id'].values
    # selected = [options[selected] for selected in cutie.select_multiple(options)]

# print the book list
