from dfs_tools_mlb.compile.static_mlb import team_info
from dfs_tools_mlb.dataframes.game_data import game_data



from functools import lru_cache
@lru_cache
def current_parks():
    parks = set()
    for k in team_info:
        if team_info[k].get('venue'):
            parks.add(team_info[k]['venue']['id'])
    return parks

park_data = game_data[game_data['venue_id'].isin(current_parks())]
park_data

# if __name__ == '__main__':
#     parks = current_parks()
#     from dfs_tools_mlb.compile import game_data
#     wind_out = game_data[game_data['wind_direction'].isin(mlb_api_codes.weather.wind_out)]
#     wind_in = game_data[game_data['wind_direction'].isin(mlb_api_codes.weather.wind_in)]
#     high_wind = game_data[game_data['wind_speed'] > game_data['wind_speed'].mean()]
#     low_wind = game_data[(game_data['wind_speed'] < game_data['wind_speed'].mean()) | (game_data['wind_speed'] == 'None')]
#     hot = game_data[game_data['temp'] > game_data['temp'].mean()]
#     cool = game_data[game_data['temp'] < game_data['temp'].mean()]
#     day = game_data[game_data['day_night'] == 'day']
#     night = game_data[game_data['day_night'] == 'night']
#     high_scoring = game_data[(game_data['runs'] > game_data['runs'].mean()) & (game_data['hits'] > game_data['hits'].mean())]
#     low_scoring = game_data[(game_data['runs'] < game_data['runs'].mean()) & (game_data['hits'] < game_data['hits'].mean())]
#     roof_closed = game_data[game_data['condition'].isin(mlb_api_codes.weather.roof_closed)]
#     roof_open = game_data[game_data['condition'].isin(mlb_api_codes.weather.roof_open)]
#     inclement_weather = game_data[game_data['condition'].isin(mlb_api_codes.weather.inclement)]
#     clear_weather = game_data[game_data['condition'].isin(mlb_api_codes.weather.clear)]
#     rain = game_data[game_data['condition'].isin(mlb_api_codes.weather.rain)]

#     high_scoring[['temp', 'wind_speed', 'fd_points']].describe()
#     low_scoring[['temp', 'wind_speed', 'fd_points']].describe()
    
    

    # wind_out['fd_points'].describe()
    # wind_in['fd_points'].describe()
    # high_wind['fd_points'].describe()
    # low_wind['fd_points'].describe()
    # hot['fd_points'].describe()
    # cool['fd_points'].describe()
    # day['fd_points'].describe()
    # night['fd_points'].describe()
    # roof_closed['fd_points'].describe()
    # roof_open['fd_points'].describe()
    # inclement_weather['fd_points'].describe()
    # clear_weather['fd_points'].describe()
    # rain['fd_points'].describe()

    # per = hot['fd_points'].mean() / game_data['fd_points'].mean()
    # per_c = cool['fd_points'].mean() / game_data['fd_points'].mean()












