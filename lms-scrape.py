import requests
import pandas as pd
import os
import json

try:
    import dryscrape
    from xvfbwrapper import Xvfb
    x = Xvfb()
    x.start()
    ds_session = dryscrape.Session()
except:
    pass

from bs4 import BeautifulSoup
from datetime import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))

#Sheet ID in sheetdb.io account
sheetid = 'l1qhwfpspvmkk'

league_details = {}
leagues = []

#Load all the active leagues
leagues = json.loads(requests.get('https://sheetdb.io/api/v1/{}/search?sheet=Leagues&Active=Yes'.format(sheetid)).content)
#Load all the divisions
all_divisions = json.loads(requests.get('https://sheetdb.io/api/v1/{}?sheet=Divisions'.format(sheetid)).content)
#Load all the active divisions
active_divisions = json.loads(requests.get('https://sheetdb.io/api/v1/{}/search?sheet=Divisions&Active=Yes&Location=Snookered - Frisco'.format(sheetid)).content)
#Load the table assignment configurations
table_assignment_configurations = json.loads(requests.get('https://sheetdb.io/api/v1/{}?sheet=Table Assignment Configuration'.format(sheetid)).content)

if True:
    print("Extracting league data " + str(datetime.now()))

    #Loop through each league URL pulling in all the divisions from drop down list and storing them in the divisions spreadsheet if they dont exist
    for league in leagues:
        html_page = requests.get(league['League URL'], headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(html_page.content, 'html.parser')
        selects = soup.find_all('select', class_='form-control')[0]
        options = selects.find_all('option')

        #Each option in the drop down
        for option in options:
            division_guid = option.attrs.get('value')
            division_name = option.text.strip()

            #If the division is in the list of active divisions store the details about it in the leagues collection
            if division_guid in [x['Division GUID'] for x in active_divisions]:
                league_details[division_guid] = dict(name=division_name, league_name=league['League Name'], league_url=league['League URL'], sanctioned_by=league['Sanctioned By'])

            #If its not in the divisions at all, add it but do not set it as active. That will be done manually by an administrator
            if division_guid not in [x['Division GUID'] for x in all_divisions]:
                data = {'data': [{
                    'Entity': league['League Name'],
                    'Division Name': division_name,
                    'Division GUID': division_guid,
                    'Active': "",
                    'Location': "",
                    'Teams': 0
                }]}
                response = requests.post('https://sheetdb.io/api/v1/{}?sheet=Divisions'.format(sheetid),
                                         data=json.dumps(data), headers={'Content-type': 'application/json'})

    def update_player_team(guid, player_name, team_name):
        #This is kind of a hack, but the way LMS stores info makes it difficult to know when you've got ALL the players on a team. This just adds a player to the team collection for the league passed in.
        standings = league_details[guid]['player_standings']
        for player in standings:
            if player[1] == player_name and player[-1] != -99:
                player.append(team_name)
                player.append(-99)
                break

    def get_match_score(league_detail, match_url):
        #LMS has a variety of league/scoring formats. This attempts to scrape the score from each submitted score sheet.
        if league_detail['sanctioned_by'] == 'BCAPL/ACS':
            try:
                if ds_session:
                    print("Visitng {}".format(match_url))
                    try:
                        ds_session.visit(match_url)
                        match_html = ds_session.body()
                        soup = BeautifulSoup(match_html, 'html.parser')
                        totals_container = soup.find_all("div", class_="round-totals-container")
                        totals_container = totals_container[-1]
                        home_totals = totals_container.find("div", class_="running-total-1 round-running-total")
                        away_totals = totals_container.find("div", class_="running-total-2 round-running-total")
                        home_score = home_totals.text
                        away_score = away_totals.text
                        print("Got score from BCA {} to {}".format(home_score, away_score))
                        return home_score, away_score
                    except Exception as e:
                        print(e)
            except:
                pass
        else:
            try:
                match_html = requests.post(match_url).content
                soup = BeautifulSoup(match_html, 'html.parser')
                table = soup.find_all("table", class_ = "table")
                if len(table) == 3:
                    table = table[2]
                    header = table.find("thead")
                    footer = table.find("tfoot")
                    if footer:
                        rows = footer.find_all("tr")
                        total_row = rows[-1]
                        total_columns = total_row.find_all('td')
                        away_score = total_columns[2].text
                        home_score = total_columns[5].text
                        return home_score, away_score
            except:
                pass
        return 0, 0

    #Loop through each division getting all the related data (schedules, standings, teams, players)
    for division in active_divisions:
        guid = division['Division GUID']
        if guid in league_details:
            print("Getting league details for {}".format(league_details[guid]['name']))
            dfs = pd.read_html("https://lms.fargorate.com/PublicReport/GenerateTeamStandingsReport/{}".format(guid))
            league_details[guid]['standings'] = []
            league_details[guid]['schedules'] = []

            schedules_html = requests.post("https://lms.fargorate.com/PublicReport/GenerateDivisionScheduleReport",
                                     data=dict(divisionId=guid)).content
            soup = BeautifulSoup(schedules_html, 'html.parser')
            root = soup.find('div', {"id": 'schedule-list'})
            items = root.find_all('div')
            schedule_date, home_team, away_team, location = None, None, None, None

            print("...getting schedules")
            for item in items:
                if 'schedule-date' in item.attrs['class']:
                    schedule_date = item.text
                if 'schedule-team-block' in item.attrs['class']:
                    teams = item.find_all('span', class_='schedule-team')
                    match_url = "{0}{1}".format("https://lms.fargorate.com", item.attrs.get('data-url'))
                    if len(teams) == 2:
                        home_team = teams[0].text
                        away_team = teams[1].text
                    location = item.find('span', class_='schedule-location').text

                    home_score, away_score = 0, 0
                    #if match_url:
                    #    from dateutil import parser
                    #    sd = parser.parse(schedule_date)
                    #    if sd < datetime.today():
                    #        home_score, away_score = get_match_score(league_details[guid], match_url)
                    league_details[guid]['schedules'].append({'Date': schedule_date, 'Home Team': home_team, 'Away Team': away_team, 'Location': location, 'Match URL': match_url, "Home Team Score": home_score, "Away Team Score": away_score})

            list_of_values = dfs[0].values.tolist()
            print("...getting standings")
            for standing in list_of_values:
                league_details[guid]['standings'].append(standing)
                dfs = pd.read_html("https://lms.fargorate.com/PublicReport/GeneratePlayerStandingsReport/{}".format(guid))
                league_details[guid]['player_standings'] = []
                for player_standing in dfs[0].values.tolist():
                    league_details[guid]['player_standings'].append(player_standing)

            print("...getting player ratings")
            dfs = pd.read_html("https://lms.fargorate.com/PublicReport/GeneratePlayerListReport/{}".format(guid))
            list_of_values = dfs[0].values.tolist()
            for player in list_of_values:
                player_name = player[0]
                player_standings = league_details[guid]['player_standings']
                for x in player_standings:
                    name_parts = x[1].split(',')
                    formatted_name = (name_parts[1] + ' ' + ' '.join(name_parts[:-1])).strip()
                    if formatted_name == player_name:
                        x.append(player[1])
                        break

            print("...getting teams")
            dfs = pd.read_html('https://lms.fargorate.com/PublicReport/GeneratePlayerStandingsByTeamReport/{}'.format(guid))
            list_of_values = dfs[0].values.tolist()
            team_name = None
            for row in list_of_values:
                if row[0] == row[1]:
                    team_name = row[0]
                    continue
                else:
                    update_player_team(guid, row[0], team_name)

    #Prepare for storing the data that has been loaded by removing all the existing data
    print("Deleting existing data...")
    response = requests.delete('https://sheetdb.io/api/v1/{}/all?sheet=Standings'.format(sheetid))
    response = requests.delete('https://sheetdb.io/api/v1/{}/all?sheet=Player Standings'.format(sheetid))
    response = requests.delete('https://sheetdb.io/api/v1/{}/all?sheet=Schedules'.format(sheetid))

    for division in active_divisions:
        try:
            details = league_details[division['Division GUID']]
            data_rows = []
            if 'standings' in details:
                #Format the data to be stored in the sheet in a way that sheetdb.io understands
                for standing in details['standings']:
                    data_array = {
                                'League Name': details['league_name'],
                                'Sanctioned By': details['sanctioned_by'],
                                'Division Name': details['name'],
                                'Division GUID': division['Division GUID'],
                                'Active': division['Active'],
                                'Location': division['Location'],
                                'Position': len(data_rows) + 1,
                                'Team Name': standing[1],
                                'Points': standing[2],
                                'Record': standing[3],
                                'Points Per Set': standing[4],
                                'Weeks': standing[5] if len(standing) == 6 else 0
                    }
                    data_rows.append(data_array)
            
            #Save the standings
            print("Saving standings...")
            if data_rows:
                response = requests.post('https://sheetdb.io/api/v1/{}?sheet=Standings'.format(sheetid), json.dumps({'data': data_rows}), headers={'Content-type': 'application/json'})

            data_rows = []
            for schedule in details['schedules']:
                data_array =  {
                                'League Name': details['league_name'],
                                'Sanctioned By': details['sanctioned_by'],
                                'Division Name': details['name'],
                                'Division GUID': division['Division GUID'],
                                'Active': division['Active'],
                                'Location': schedule['Location'],
                                'Date': schedule['Date'],
                                'Home Team': schedule['Home Team'],
                                'Away Team': schedule['Away Team'],
                                'Match URL': schedule['Match URL'],
                                'Home Team Score': schedule['Home Team Score'],
                                'Away Team Score': schedule['Away Team Score']
                }
                data_rows.append(data_array)
            
            #Save the schedules
            print("Saving schedules...")
            if data_rows:
                response = requests.post('https://sheetdb.io/api/v1/{}?sheet=Schedules'.format(sheetid), json.dumps({'data': data_rows}),
                                     headers={'Content-type': 'application/json'})

            #Depending on the league sanctioning, the format of the data from LMS can be completely different. This attempts to parse out the league standings
            try:
                data_rows = []
                for ps in details['player_standings']:
                    #print(ps)
                    if details['sanctioned_by'] == 'BCAPL/ACS':
                        br_column = 6
                        wz_column = 7
                        tr_column = 8
                        wb_column = 9
                    else:
                        br_column = 5
                        wz_column = 6
                        tr_column = 7
                        wb_column = 8
                    data_array = {
                    'League Name': details['league_name'],
                    'Sanctioned By': details['sanctioned_by'],
                    'Division Name': details['name'],
                    'Division GUID': division['Division GUID'],
                    'Active': division['Active'],
                    'Location': division['Location'],
                    'Position': len(data_rows) + 1,
                    'Name': ps[1],
                    'MVP': ps[2],
                    'Team Name': ps[-2],
                    'Points': ps[3],
                    'Break And Runs': ps[br_column],
                    'Win Zips': ps[wz_column],
                    'Table Runs': ps[tr_column],
                    'Win On Breaks': ps[wb_column],
                    'Rating': ps[-3]
                }
                    data_rows.append(data_array)
                response = requests.post('https://sheetdb.io/api/v1/{}?sheet=Player Standings'.format(sheetid), json.dumps({'data': data_rows}),
                                     headers={'Content-type': 'application/json'})
            except Exception as e:
                print(e)

            #Save the sandings
            if 'standings' in details:
                response = requests.put('https://sheetdb.io/api/v1/{}/Division GUID/{}?sheet=Divisions'.format(sheetid,
                                                                                                           division['Division GUID']),
                                    dict(Teams=len(details['standings'])))
        except Exception as e:
            print("Error updating sheet {} was {}".format(details['name'], e))

#assign matches on Monday
from datetime import datetime
current_date = datetime.today()
current_year, current_week_num, current_day_num = current_date.isocalendar()

#Run on mondays (at 9am)
if current_day_num == 1:
    print("Generating table assignments " + str(current_date))
    from dateutil import parser
    import re

    #Get all the schedules for active leagues
    all_schedules = json.loads(requests.get('https://sheetdb.io/api/v1/{}/search?sheet=Schedules&Active=Yes'.format(sheetid)).content)
    #Just get the ones scheduled at Snookered
    snookered_schedules = [x for x in all_schedules if 'Snookered' in x['Location']]
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    #Get the collection of available table clusters from the Table Assignment Configuration sheet. Configurations are associated by day number AND sanctioning type so multiple league types can play on different tables at the same time
    available_table_clusters = {}
    for c in table_assignment_configurations:
        config = available_table_clusters.get(days_of_week.index(c['Day Of Week']) + 1, {})
        sanction_clusters = config.get(c['Sanctioned By'], [])
        sanction_clusters.append(c['Table Cluster'])
        config[c['Sanctioned By']] = sanction_clusters
        available_table_clusters[days_of_week.index(c['Day Of Week']) + 1] = config

    #This is just to ensure the owners ALWAYS get the restaurant side so we arent bothered by customers all night ;) This can be removed later
    #dedicated_table_clusters =  {'We Were On A Break' : "15 / 16 / 17"}
    reduced_matches = dict()
    sanction_priority = ['BCAPL/ACS', 'BCAPL/USAPL'] #Schedule BCA matches first because they require 2-table clusters, USAPL requires 3

    #Loop over all the scheduled matches and reduce DOUBLE JEOPARDY matches down to 1 match. This is when teams play 8 and 9 ball at the same time. In LMS that shows up as 2 different matches, this consolidates
    #them into 1 so they use the same table cluster for both matches.
    for schedule in snookered_schedules:
        sd = parser.parse(schedule['Date'])
        year, week_num, day_of_week = sd.isocalendar()
        if year == current_year and week_num == current_week_num:
            home_team = re.sub(r'\([^()]*\)', '', schedule['Home Team']).strip().encode('utf8')
            away_team = re.sub(r'\([^()]*\)', '', schedule['Away Team']).strip().encode('utf8')

            if away_team in [b'Bye', b'Bye Team'] or home_team in [b'Bye', b'Bye Team']: # Bye matches dont need to be scheduled
                continue

            slug = "{}.{}.{}".format(home_team, away_team, schedule['Date'])
            if slug not in reduced_matches:
                schedule['Day Of Week'] = day_of_week
                schedule['Date Raw'] = sd
                schedule['Clean Home Team'] = home_team.decode('utf8')
                schedule['Clean Away Team'] = away_team.decode('utf8')
                reduced_matches[slug] = schedule
            else:
                reduced_matches[slug]['Alternate Division GUID'] = schedule['Division GUID']

    #Loop over the reduced matches grabbing an available table cluster and removing it from the list of available table clusters
    final_matches = []
    for key, value in reduced_matches.items():
        for sanctioned_by in sanction_priority:
            if value['Sanctioned By'] == sanctioned_by:
                tables = available_table_clusters[value['Day Of Week']].get(value['Sanctioned By'], None)
                if len(tables) > 0:
                    value['Tables'] = tables.pop(0)
                else:
                    value['Tables'] = "?"
        final_matches.append(value)

    #Get the list of open tables for each day of the week
    open_tables = {}
    for key, value in available_table_clusters.items():
        if 'Open' in value:
            open_tables[key] = value['Open'][0]

    #Create the open table collection to be stored for each day in the format that sheetdb.io likes
    open_table_matches = {}
    for match in final_matches:
        day_of_week = match['Day Of Week']
        if day_of_week in open_tables and day_of_week not in open_table_matches:
            open_table_match = {
                'Division GUID' : None,
                'Alternate Division GUID': None,
                'Sanctioned By': "Open",
                'Location': match['Location'],
                'Date': match['Date'],
                'Date Raw': match['Date Raw'],
                'Day Of Week': day_of_week,
                'Clean Home Team': "Open",
                'Clean Away Team': "Open",
                'Tables': open_tables[day_of_week]
            }
            open_table_matches[day_of_week] = open_table_match

    #Append the open tables to the final list of matches
    if open_table_matches:
        final_matches.extend(open_table_matches.values())

    #Sort the matches in order by date
    reduced_matches = sorted(final_matches, key=lambda d: d['Date Raw'])

    #Create the final collection in the sheetdb.io format 
    data_rows = []
    for value in reduced_matches:
        data_rows.append({
            'Division GUID': value['Division GUID'],
            'Alternate Division GUID': value.get('Alternate Division GUID', ''),
            'Sanctioned By': value['Sanctioned By'],
            'Location': value['Location'],
            'Date': value['Date'],
            'Day Of Week': days_of_week[value['Day Of Week']-1],
            'Team 1': value['Clean Home Team'],
            'Team 2': value['Clean Away Team'],
            'Tables': value['Tables']
        })

    #Clear the sheet and store the data
    requests.delete('https://sheetdb.io/api/v1/{}/all?sheet=Table Assignments'.format(sheetid))
    response = requests.post('https://sheetdb.io/api/v1/{}?sheet=Table Assignments'.format(sheetid),
                                 json.dumps({'data': data_rows}), headers={'Content-type': 'application/json'})




