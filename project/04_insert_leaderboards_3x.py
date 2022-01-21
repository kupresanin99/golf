import sqlite3
import requests
import pprint
import secrets

pp = pprint.PrettyPrinter()

headers = {'Ocp-Apim-Subscription-Key': f'{secrets.api_key}'}
leaderboard_url = f'https://api.sportsdata.io/golf/v2/json/Leaderboard/'
db_path = "./data/golf.db"
conn = sqlite3.connect(db_path)

conn.execute("""DROP TABLE IF EXISTS leaderboard_tournament;""")
conn.execute("""
        CREATE TABLE leaderboard_tournament
        (
        player_id INT NOT NULL
        ,tourn_id INT NOT NULL
        ,player_tourn_id INT NOT NULL
        ,birdies REAL
        ,bogey_free_rounds REAL
        ,bogeys REAL
        ,bounce_back_count REAL
        ,consecutive_birdies_or_better_count REAL
        ,double_bogeys REAL
        ,double_eagles REAL
        ,draft_kings_salary REAL
        ,eagles REAL
        ,earnings REAL
        ,fan_duel_salary REAL
        ,fantasy_points_draft_kings REAL
        ,fantasy_points_fan_duel REAL
        ,fed_ex_points REAL
        ,hole_in_ones REAL
        ,made_cut REAL
        ,odds_to_win REAL
        ,pars REAL
        ,rank REAL
        ,rounds_under_70 REAL
        ,rounds_5_plus_birdies REAL
        ,streaks_3_plus_birdies REAL
        ,streaks_4_plus_birdies REAL
        ,streaks_5_plus_birdies REAL
        ,streaks_6_plus_birdies REAL
        ,tee_time TEXT
        ,total_score REAL
        ,total_strokes REAL
        ,total_through REAL
        ,triple_bogeys REAL
        ,win REAL
        ,worse_than_double_bogey REAL
        ,worse_than_triple_bogey REAL
        ,PRIMARY KEY (player_id, tourn_id, player_tourn_id));""")

conn.execute("""DROP TABLE IF EXISTS leaderboard_round;""")
conn.execute("""
        CREATE TABLE leaderboard_round
        (
        player_tourn_id INT NOT NULL
        ,player_round_id INT NOT NULL

        ,birdies REAL
        ,bogey_free INT
        ,bogeys REAL
        ,bounce_back_count REAL
        ,consecutive_birdies_or_better_count REAL
        ,double_bogeys REAL
        ,double_eagles REAL
        ,eagles REAL
        ,hole_in_ones REAL
        ,includes_streak_3_plus_birdies INT
        ,includes_streak_4_plus_birdies INT
        ,includes_streak_5_plus_birdies INT
        ,includes_streak_6_plus_birdies INT
        ,longest_birdie_streak REAL
        ,pars REAL
        ,round_number INT
        ,score INT
        ,tee_time TEXT
        ,triple_bogies REAL
        ,worse_than_double_bogey REAL
        ,worse_than_triple_bogey REAL
        ,PRIMARY KEY (player_tourn_id, player_round_id)
        );""")

conn.execute("""DROP TABLE IF EXISTS leaderboard_hole;""")
conn.execute("""
        CREATE TABLE leaderboard_hole
        (
        player_round_id INT NOT NULL
        ,hole INT NOT NULL

        ,birdie INT
        ,bogey INT
        ,double_bogey INT
        ,double_eagle INT
        ,eagle INT
        ,hole_in_one INT
        ,hole_par INT
        ,par INT
        ,score INT
        ,worse_than_double_bogey INT
        ,PRIMARY KEY (player_round_id, hole)
        );""")

cur = conn.cursor()
cur.execute("SELECT DISTINCT tourn_id FROM tournament WHERE format = 'Stroke'")
tourn_ids = cur.fetchall()
num_tourns = len(tourn_ids)
for tourn_id in tourn_ids:
  my_tourn_id = str(tourn_id[0])
  print(f"""Tourns remaining: {num_tourns}""")
  num_tourns -= 1
  print(f"""Starting tourn {tourn_id}""")
  player_results = requests.get(f'{leaderboard_url}{my_tourn_id}', headers=headers).json()

  for player in player_results['Players']:
    if len(player['Rounds']) > 0:
      if player['PlayerID'] is not None:
        player_id = int(player['PlayerID'])
      else:
        player_id = -999

      if player['TournamentID'] is not None:
        tourn_id = int(player['TournamentID'])
      else:
        player_id = -999

      if player['PlayerTournamentID'] is not None:
        player_tourn_id = int(player['PlayerTournamentID'])
      else:
        player_tourn_id = -999

      if player['Birdies'] is not None:
        birdies = player['Birdies']
      else:
        birdies = -999

      if player['BogeyFreeRounds'] is not None:
        bogey_free_rounds = player['BogeyFreeRounds']
      else:
        bogey_free_rounds = -999

      if player['Bogeys'] is not None:
        bogeys = player['Bogeys']
      else:
        bogeys = -999

      if player['BounceBackCount'] is not None:
        bounce_back_count = player['BounceBackCount']
      else:
        bounce_back_count = -999

      if player['ConsecutiveBirdieOrBetterCount'] is not None:
        consecutive_birdies_or_better_count = player['ConsecutiveBirdieOrBetterCount']
      else:
        consecutive_birdies_or_better_count = -999

      if player['DoubleBogeys'] is not None:
        double_bogeys = player['DoubleBogeys']
      else:
        double_bogeys = -999

      if player['DoubleEagles'] is not None:
        double_eagles = player['DoubleEagles']
      else:
        double_eagles = -999

      if player['DraftKingsSalary'] is not None:
        draft_kings_salary = player['DraftKingsSalary']
      else:
        draft_kings_salary = -999

      if player['Eagles'] is not None:
        eagles = player['Eagles']
      else:
        eagles = -999

      if player['Earnings'] is not None:
        earnings = player['Earnings']
      else:
        earnings = -999

      if player['FanDuelSalary'] is not None:
        fan_duel_salary = player['FanDuelSalary']
      else:
        fan_duel_salary = -999

      if player['FantasyPointsDraftKings'] is not None:
        fantasy_points_draft_kings = player['FantasyPointsDraftKings']
      else:
        fantasy_points_draft_kings = -999

      if player['FantasyPointsFanDuel'] is not None:
        fantasy_points_fan_duel = player['FantasyPointsFanDuel']
      else:
        fantasy_points_fan_duel = -999

      if player['FedExPoints'] is not None:
        fed_ex_points = player['FedExPoints']
      else:
        fed_ex_points = -999

      if player['HoleInOnes'] is not None:
        hole_in_ones = player['HoleInOnes']
      else:
        hole_in_ones = -999

      if player['MadeCut'] is not None:
        made_cut = player['MadeCut']
      else:
        made_cut = -999

      if player['OddsToWin'] is not None:
        odds_to_win = player['OddsToWin']
      else:
        odds_to_win = -999

      if player['Pars'] is not None:
        pars = player['Pars']
      else:
        pars = -999

      if player['Rank'] is not None:
        rank = player['Rank']
      else:
        rank = -999

      if player['RoundsUnderSeventy'] is not None:
        rounds_under_70 = player['RoundsUnderSeventy']
      else:
        rounds_under_70 = -999

      if player['RoundsWithFiveOrMoreBirdiesOrBetter'] is not None:
        rounds_5_plus_birdies = player['RoundsWithFiveOrMoreBirdiesOrBetter']
      else:
        rounds_5_plus_birdies = -999

      if player['StreaksOfThreeBirdiesOrBetter'] is not None:
        streaks_3_plus_birdies = player['StreaksOfThreeBirdiesOrBetter']
      else:
        streaks_3_plus_birdies = -999

      if player['StreaksOfFourBirdiesOrBetter'] is not None:
        streaks_4_plus_birdies = player['StreaksOfFourBirdiesOrBetter']
      else:
        streaks_4_plus_birdies = -999

      if player['StreaksOfFiveBirdiesOrBetter'] is not None:
        streaks_5_plus_birdies = player['StreaksOfFiveBirdiesOrBetter']
      else:
        streaks_5_plus_birdies = -999

      if player['StreaksOfSixBirdiesOrBetter'] is not None:
        streaks_6_plus_birdies = player['StreaksOfSixBirdiesOrBetter']
      else:
        streaks_6_plus_birdies = -999

      if player['TeeTime'] is not None:
        tee_time = player['TeeTime']
      else:
        tee_time = ''

      if player['TotalScore'] is not None:
        total_score = player['TotalScore']
      else:
        total_score = -999

      if player['TotalStrokes'] is not None:
        total_strokes = player['TotalStrokes']
      else:
        total_strokes = -999

      if player['TotalThrough'] is not None:
        total_through = player['TotalThrough']
      else:
        total_through = -999

      if player['TripleBogeys'] is not None:
        triple_bogeys = player['TripleBogeys']
      else:
        triple_bogeys = -999

      if player['Win'] is not None:
        win = player['Win']
      else:
        win = -999

      if player['WorseThanDoubleBogey'] is not None:
        worse_than_double_bogey = player['WorseThanDoubleBogey']
      else:
        worse_than_double_bogey = -999

      if player['WorseThanTripleBogey'] is not None:
        worse_than_triple_bogey = player['WorseThanTripleBogey']
      else:
        worse_than_triple_bogey = -999

      insert_leaderboard_tournament = f"""

                INSERT INTO leaderboard_tournament 
                (   player_id
                ,tourn_id
                ,player_tourn_id
                ,birdies
                ,bogey_free_rounds
                ,bogeys
                ,bounce_back_count
                ,consecutive_birdies_or_better_count
                ,double_bogeys
                ,double_eagles
                ,draft_kings_salary
                ,eagles
                ,earnings
                ,fan_duel_salary
                ,fantasy_points_draft_kings
                ,fantasy_points_fan_duel
                ,fed_ex_points
                ,hole_in_ones
                ,made_cut
                ,odds_to_win
                ,pars
                ,rank
                ,rounds_under_70
                ,rounds_5_plus_birdies
                ,streaks_3_plus_birdies
                ,streaks_4_plus_birdies
                ,streaks_5_plus_birdies
                ,streaks_6_plus_birdies
                ,tee_time
                ,total_score
                ,total_strokes
                ,total_through
                ,triple_bogeys
                ,win
                ,worse_than_double_bogey
                ,worse_than_triple_bogey)
                VALUES
                (
                {player_id}
                ,{tourn_id}
                ,{player_tourn_id}
                ,{birdies}
                ,{bogey_free_rounds}
                ,{bogeys}
                ,{bounce_back_count}
                ,{consecutive_birdies_or_better_count}
                ,{double_bogeys}
                ,{double_eagles}
                ,{draft_kings_salary}
                ,{eagles}
                ,{earnings}
                ,{fan_duel_salary}
                ,{fantasy_points_draft_kings}
                ,{fantasy_points_fan_duel}
                ,{fed_ex_points}
                ,{hole_in_ones}
                ,{made_cut}
                ,{odds_to_win}
                ,{pars}
                ,{rank}
                ,{rounds_under_70}
                ,{rounds_5_plus_birdies}
                ,{streaks_3_plus_birdies}
                ,{streaks_4_plus_birdies}
                ,{streaks_5_plus_birdies}
                ,{streaks_6_plus_birdies}
                ,'{tee_time}'
                ,{total_score}
                ,{total_strokes}
                ,{total_through}
                ,{triple_bogeys}
                ,{win}
                ,{worse_than_double_bogey}
                ,{worse_than_triple_bogey})"""

      conn.execute(insert_leaderboard_tournament)
      conn.commit()

      for round in player['Rounds']:

        if round['PlayerTournamentID'] is not None:
          player_tourn_id = int(round['PlayerTournamentID'])
        else:
          player_tourn_id = -999

        if round['PlayerRoundID'] is not None:
          player_round_id = int(round['PlayerRoundID'])
        else:
          player_round_id = -999

        if round['Birdies'] is not None:
          birdies = round['Birdies']
        else:
          birdies = -999

        if round['BogeyFree'] == True:
          bogey_free = 1
        else:
          bogey_free = 0

        if round['Bogeys'] is not None:
          bogeys = round['Bogeys']
        else:
          bogeys = -999

        if round['BounceBackCount'] is not None:
          bounce_back_count = round['BounceBackCount']
        else:
          bounce_back_count = -999

        if round['ConsecutiveBirdieOrBetterCount'] is not None:
          consecutive_birdies_or_better_count = round['ConsecutiveBirdieOrBetterCount']
        else:
          consecutive_birdies_or_better_count = -999

        if round['DoubleBogeys'] is not None:
          double_bogeys = round['DoubleBogeys']
        else:
          double_bogeys = -999

        if round['DoubleEagles'] is not None:
          double_eagles = round['DoubleEagles']
        else:
          double_eagles = -999

        if round['Eagles'] is not None:
          eagles = round['Eagles']
        else:
          eagles = -999

        if round['HoleInOnes'] is not None:
          hole_in_ones = round['HoleInOnes']
        else:
          hole_in_ones = -999

        if round['IncludesStreakOfThreeBirdiesOrBetter'] == True:
          includes_streak_3_plus_birdies = 1
        else:
          includes_streak_3_plus_birdies = 0

        if round['IncludesStreakOfFourBirdiesOrBetter'] == True:
          includes_streak_4_plus_birdies = 1
        else:
          includes_streak_4_plus_birdies = 0

        if round['IncludesStreakOfFiveBirdiesOrBetter'] == True:
          includes_streak_5_plus_birdies = 1
        else:
          includes_streak_5_plus_birdies = 0

        if round['IncludesStreakOfSixBirdiesOrBetter'] == True:
          includes_streak_6_plus_birdies = 1
        else:
          includes_streak_6_plus_birdies = 0

        if round['LongestBirdieOrBetterStreak'] is not None:
          longest_birdie_streak = round['LongestBirdieOrBetterStreak']
        else:
          longest_birdie_streak = -999

        if round['Pars'] is not None:
          pars = round['Pars']
        else:
          pars = -999

        if round['Number'] is not None:
          round_number = int(round['Number'])
        else:
          round_number = -999

        if round['Score'] is not None:
          score = round['Score']
        else:
          score = -999

        if round['TeeTime'] is not None:
          tee_time = round['TeeTime']
        else:
          tee_time = ''

        if round['TripleBogeys'] is not None:
          triple_bogeys = round['TripleBogeys']
        else:
          triple_bogeys = -999

        if round['WorseThanDoubleBogey'] is not None:
          worse_than_double_bogey = round['WorseThanDoubleBogey']
        else:
          worse_than_double_bogey = -999

        if round['WorseThanTripleBogey'] is not None:
          worse_than_triple_bogey = round['WorseThanTripleBogey']
        else:
          worse_than_triple_bogey = -999

        insert_leaderboard_round = f"""

                INSERT INTO leaderboard_round
                (   
                player_tourn_id
                ,player_round_id
                ,birdies
                ,bogey_free
                ,bogeys
                ,bounce_back_count
                ,consecutive_birdies_or_better_count
                ,double_bogeys
                ,double_eagles
                ,eagles
                ,hole_in_ones
                ,includes_streak_3_plus_birdies
                ,includes_streak_4_plus_birdies
                ,includes_streak_5_plus_birdies
                ,includes_streak_6_plus_birdies
                ,longest_birdie_streak
                ,pars
                ,round_number
                ,score
                ,tee_time
                ,triple_bogies
                ,worse_than_double_bogey
                ,worse_than_triple_bogey)
                VALUES
                (
                {player_tourn_id}
                ,{player_round_id}
                ,{birdies}
                ,{bogey_free}
                ,{bogeys}
                ,{bounce_back_count}
                ,{consecutive_birdies_or_better_count}
                ,{double_bogeys}
                ,{double_eagles}
                ,{eagles}
                ,{hole_in_ones}
                ,{includes_streak_3_plus_birdies}
                ,{includes_streak_4_plus_birdies}
                ,{includes_streak_5_plus_birdies}
                ,{includes_streak_6_plus_birdies}
                ,{longest_birdie_streak}
                ,{pars}
                ,{round_number}
                ,{score}
                ,'{tee_time}'
                ,{triple_bogeys}
                ,{worse_than_double_bogey}
                ,{worse_than_triple_bogey})"""

        conn.execute(insert_leaderboard_round)
        conn.commit()

      for round in player['Rounds']:

        holes = round['Holes']
        for hole in holes:
          
          if hole['PlayerRoundID'] is not None:
            player_round_id = int(hole['PlayerRoundID'])
          else:
            player_round_id = -999

          if hole['Number'] is not None:
            my_hole = int(hole['Number'])
          else:
            my_hole = -999

          if hole['Birdie'] == True:
            birdie = 1
          else:
            birdie = 0

          if hole['Bogey'] == True:
            bogey = 1
          else:
            bogey = 0

          if hole['DoubleBogey'] == True:
            double_bogey = 1
          else:
            double_bogey = 0

          if hole['DoubleEagle'] == True:
            double_eagle = 1
          else:
            double_eagle = 0

          if hole['Eagle'] == True:
            eagle = 1
          else:
            eagle = 0

          if hole['HoleInOne'] == True:
            hole_in_one = 1
          else:
            hole_in_one = 0

          if hole['Par'] is not None:
            hole_par = int(hole['Par'])
          else:
            hole_par = -999

          if hole['IsPar'] == True:
            par = 1
          else:
            par = 0

          if hole['Score'] is not None:
            score = int(hole['Score'])
          else:
            score = -999

          if hole['WorseThanDoubleBogey'] == True:
            worse_than_double_bogey = 1
          else:
            worse_than_double_bogey = 0

          insert_leaderboard_hole = f"""

                INSERT INTO leaderboard_hole
                (   
                player_round_id
                ,hole
                ,birdie
                ,bogey
                ,double_bogey
                ,double_eagle
                ,eagle
                ,hole_in_one
                ,hole_par
                ,par
                ,score
                ,worse_than_double_bogey)
                VALUES
                (
                {player_round_id}
                ,{my_hole}
                ,{birdie}
                ,{bogey}
                ,{double_bogey}
                ,{double_eagle}
                ,{eagle}
                ,{hole_in_one}
                ,{par}
                ,{round_number}
                ,{score}
                ,{worse_than_double_bogey})"""

          conn.execute(insert_leaderboard_hole)
          conn.commit()

conn.close()