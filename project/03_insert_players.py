import sqlite3
import requests
import pprint
import secrets

pp = pprint.PrettyPrinter()

headers = {'Ocp-Apim-Subscription-Key': f'{secrets.api_key}'}
player_url = 'https://api.sportsdata.io/golf/v2/json/Players'
db_path = "./data/golf.db"
conn = sqlite3.connect(db_path)

conn.execute("""DROP TABLE IF EXISTS player;""")
conn.execute("""
        CREATE TABLE player
        (
        player_id INT PRIMARY KEY NOT NULL
        ,last_name TEXT
        ,first_name TEXT
        ,draft_kings_name TEXT
        ,fan_duel_name TEXT
        ,draft_kings_id INT
        ,fan_duel_id INT
        ,fantasy_draft_id INT
        ,pga_id INT
        ,yahoo_id INT
        ,sports_radar_id TEXT);""")

players = requests.get(f'{player_url}', headers=headers).json()

for player in players:
    try:
        if player['PlayerID'] is not None:
            player_id = int(player['PlayerID'])
        else:
            player_id = -999

        if player['LastName'] is not None:
            last_name = player['LastName']
        else:
            last_name = ''

        if player['FirstName'] is not None:
            first_name = player['FirstName']
        else:
            first_name = ''

        if player['DraftKingsName'] is not None:
            draft_kings_name = player['DraftKingsName']
        else:
            draft_kings_name = ''

        if player['FanDuelName'] is not None:
            fan_duel_name = player['FanDuelName']
        else:
            fan_duel_name = ''

        if player['DraftKingsPlayerID'] is not None:
            draft_kings_id = int(player['DraftKingsPlayerID'])
        else:
            draft_kings_id = -999

        if player['FanDuelPlayerID'] is not None:
            fan_duel_id = int(player['FanDuelPlayerID'])
        else:
            fan_duel_id = -999

        if player['FantasyDraftPlayerID'] is not None:
            fantasy_draft_id = int(player['FantasyDraftPlayerID'])
        else:
            fantasy_draft_id = -999

        if player['PgaTourPlayerID'] is not None:
            pga_id = int(player['PgaTourPlayerID'])
        else:
            pga_id = -999

        if player['YahooPlayerID'] is not None:
            yahoo_id = int(player['YahooPlayerID'])
        else:
            yahoo_id = -999

        if player['SportRadarPlayerID'] is not None:
            sports_radar_id = player['SportRadarPlayerID']
        else:
            sports_radar_id = ''

        insert_statement = f"""
    
            INSERT INTO player 
            (   player_id
                ,last_name
                ,first_name
                ,draft_kings_name
                ,fan_duel_name
                ,draft_kings_id
                ,fan_duel_id
                ,fantasy_draft_id
                ,pga_id
                ,roto_id
                ,yahoo_id
                ,sports_radar_id)
            VALUES
                ({player_id}
                ,'{last_name}'
                ,'{first_name}'
                ,'{draft_kings_name}'
                ,'{fan_duel_name}'
                ,{draft_kings_id}
                ,{fan_duel_id}
                ,{fantasy_draft_id}
                ,{pga_id}
                ,{roto_id}
                ,{yahoo_id}
                ,'{sports_radar_id}')"""

        conn.execute(insert_statement)
        conn.commit()
    except:
        None
conn.close()