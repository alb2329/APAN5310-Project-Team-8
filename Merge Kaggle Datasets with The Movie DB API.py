#!/usr/bin/env python
# coding: utf-8

# In[136]:


import requests # to make TMDB API calls
import locale
locale.setlocale( locale.LC_ALL, '' )

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

api_key = "221eb71e28fb8ae8e94a52bcfe1390e5"
movie_api_url = "https://api.themoviedb.org/3/discover/movie?api_key="
tv_api_url = "https://api.themoviedb.org/3/discover/tv?api_key="


# In[137]:


netflix = pd.read_csv('netflix_cleaned_v2.csv')
platformnetflix = ['Netflix'] *  netflix.shape[0]
netflix['platform'] = platformnetflix
print(netflix)


# In[138]:


hulu = pd.read_csv('hulu_cleaned.csv')
platformhulu = ['Hulu'] *  hulu.shape[0]
hulu['platform'] = platformhulu
print(hulu)


# In[139]:


amazon = pd.read_csv('amazon_prime_cleaned_v2.csv')
platformamazon = ['Amazon'] *  amazon.shape[0]
amazon['platform'] = platformamazon
print(amazon)


# In[140]:


disney = pd.read_csv('disney_plus_cleaned_v2.csv')
platformdisney = ['Disney'] *  disney.shape[0]
disney['platform'] = platformdisney
print(disney)


# In[141]:


frames = [netflix, hulu, amazon, disney]
result = pd.concat(frames)
print(result)


# In[142]:


grouped = result.groupby(result.type)
tv = grouped.get_group("TV Show")
print(tv)


# In[143]:


movie = grouped.get_group("Movie")
print(movie)


# In[153]:


tv_api_url = 'https://api.themoviedb.org/3/discover/tv?api_key=221eb71e28fb8ae8e94a52bcfe1390e5&language=en-US&include_adult=false&include_video=false'

tv_api = requests.get(tv_api_url).json()
tv_results = tv_api["results"]
for page in range(1, 200):
    tv_api = requests.get(tv_api_url + f"&page={page}").json()
    tv_results.extend(tv_api["results"])
print(tv_results)


# In[154]:


movie_api_url = 'https://api.themoviedb.org/3/discover/movie?api_key=221eb71e28fb8ae8e94a52bcfe1390e5&language=en-US&include_adult=false&include_video=false'

movie_api = requests.get(movie_api_url).json()
movie_results = movie_api["results"]
for page in range(1, 200):
    movie_api = requests.get(movie_api_url + f"&page={page}").json()
    movie_results.extend(movie_api["results"])
print(movie_results)


# In[155]:


m_columns = ['film', 'revenue', 'budget','vote_average']
movie_df = pd.DataFrame(columns=m_columns)


# In[156]:


t_columns = ['show', 'episode_run_time','number_of_episodes','vote_average']
tv_df = pd.DataFrame(columns=t_columns)


# In[157]:


# for each of the films make an api call for that specific movie to return the budget, vote_average, and revenue
for film in movie_results:
    film_revenue = requests.get('https://api.themoviedb.org/3/movie/'+ str(film['id']) +'?api_key='+ api_key)
    film_revenue = film_revenue.json()
    movie_df.loc[len(movie_df)]=[film['title'],film_revenue['revenue'],film_revenue['budget'],film_revenue['vote_average']]


# In[158]:


print(movie_df)


# In[160]:


# for each of the shows make an api call for that specific tv show to return the episode_run_time, number_of_episodes, and vote_average
for show in tv_results:
    show_extra = requests.get('https://api.themoviedb.org/3/tv/'+ str(show['id']) +'?api_key='+ api_key)
    show_extra = show_extra.json()
    tv_df.loc[len(tv_df)]=[show['name'],show_extra['episode_run_time'],show_extra['number_of_episodes'],show_extra['vote_average']]


# In[161]:


print(tv_df)


# In[193]:


movie_df = movie_df.rename(columns={"film": "title"})


# In[194]:


tv_df = tv_df.rename(columns={"show": "title"})


# In[198]:


full_movie_df = pd.merge(movie, movie_df, on="title", how="left")


# In[199]:


full_tv_df = pd.merge(tv, tv_df, on="title", how="left")


# In[200]:


frames_full = [full_movie_df, full_tv_df]
final_data = pd.concat(frames_full)


# In[201]:


print(final_data)


# In[212]:


final_data.to_excel("output.xlsx")

