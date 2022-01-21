import requests
import pprint
import secrets
pp = pprint.PrettyPrinter()
headers = {'Ocp-Apim-Subscription-Key': f'{secrets.api_key}'}

endpoint4 = 'https://api.sportsdata.io/golf/v2/json/Leaderboard/'
tournament_id = '445'                                               # 2021 British Open
tournament = requests.get(f'{endpoint4}{tournament_id}', headers=headers).json()

#print(tournament.keys())
#pp.pprint(tournament['Tournament'])



pp.pprint(tournament['Players'][-1]['Rounds'])
# for player in tournament['Players']:
#   print(len(player['Rounds']))

# endpoint2 = 'https://api.sportsdata.io/golf/v2/json/DfsSlatesByTournament/'
# tournament_id = '449'
# r2 = requests.get(f'{endpoint2}{tournament_id}', headers=headers).json()
# print(r2)
