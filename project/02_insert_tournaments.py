import sqlite3
import requests
import pprint
import secrets

pp = pprint.PrettyPrinter()


headers = {'Ocp-Apim-Subscription-Key': f'{secrets.api_key}'}
tourn_url = 'https://api.sportsdata.io/golf/v2/json/Tournaments/'
seasons = ['2022',
           '2021',
           '2020',
           '2019']
db_path = "./data/golf.db"
conn = sqlite3.connect(db_path)

conn.execute("""DROP TABLE IF EXISTS tournament;""")

conn.execute("""
    CREATE TABLE tournament
    (
    tourn_id INT PRIMARY KEY NOT NULL
    ,tourn_name TEXT
    ,start_date TEXT
    ,end_date TEXT
    ,location TEXT
    ,city TEXT
    ,state TEXT
    ,zip TEXT
    ,country TEXT
    ,format TEXT
    ,par INT
    ,yards INT
    ,purse INT);""")

for season in seasons:
    try:
        tournaments = requests.get(
            f'{tourn_url}{season}', headers=headers).json()

        for i in range(len(tournaments)):
            tourn = tournaments[i]
            tourn_id = int(tourn['TournamentID'])
            tourn_name = tourn['Name']
            start_date = tourn['StartDate']
            end_date = tourn['EndDate']
            venue = tourn['Venue']
            location = tourn['Location']
            city = tourn['City']
            state = tourn['State']
            zip = tourn['ZipCode']
            country = tourn['Country']
            format = tourn['Format']

            if tourn['Par'] is not None:
                par = int(tourn['Par'])
            else:
                par = 0

            if tourn['Yards'] is not None:
                yards = int(tourn['Yards'])
            else:
                yards = 0

            if tourn['Purse'] is not None:
                purse = int(tourn['Purse'])
            else:
                purse = 0

            insert_statement = f"""
            
            INSERT INTO tournament 
            (   tourn_id 
                ,tourn_name 
                ,start_date 
                ,end_date
                ,location
                ,city 
                ,state 
                ,zip 
                ,country 
                ,format 
                ,par 
                ,yards 
                ,purse)
            VALUES
                ({tourn_id}
                ,"{tourn_name}"
                ,'{start_date}'
                ,'{end_date}'
                ,'{location}'
                ,'{city}'
                ,'{state}'
                ,'{zip}'
                ,'{country}'
                ,'{format}'
                ,{par}
                ,{yards}
                ,{purse})"""

            conn.execute(insert_statement)
            conn.commit()
    except:
        None
conn.close()
