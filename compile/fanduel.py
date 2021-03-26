fd_scoring = {
    'hitters': {
        'base': 3,
        'rbi' : 3.5,
        'run' : 3.2,
        'sb' : 6,
        'hr': 18.7,
        
        },
    'pitchers': {
        'inning': 3,
        'out': 1,
        'k' : 3,
        'er': -3,
        'qs': 4,
        'win': 6
        }
    }

fd_weights = {
    'venue_p': .4,
    'temp_p': .4,
    'ump_p': .2,
    'venue_h': .4,
    'temp_h': .4,
    'ump_h': .2
    }
