#This script Imports Game Data from ESPN, and Odds from the ODDS-API, and then imports them into a MySQL table, example in workbench here https://puu.sh/HOKCj/ce199eec8e.png
import mysql.connector
import requests
import json
import datetime
import time


#Connection to the MYSQL Server.
mydb = mysql.connector.connect(
  host="",
  user="",
  password="",
  database="basketbet_data"
  )
mycursor = mydb.cursor()


#Games List.
allGames=[]
#Gets the game Data from ESPN API given the link.
def newGetter(gameDay):
    #Json Response for YESTERDAY.
    response = requests.get(gameDay).json()
    gameData = response["events"]
    
    #Loop through to collect GameDay data.
    a=0
    while a < len(gameData):
        game = str(gameData[a]['name'])
        game_ID = str(gameData[a]['id'])
        game_Date = str(gameData[a]['date'][:-7])
        game_Time = str(gameData[a]['date'][11:-1])
        game_Period = str(gameData[a]['status']['period'])
        game_Status = str(gameData[a]['status']['type']['description'])
        home_Score = str(gameData[a]['competitions'][0]['competitors'][0]['score'])
        away_Score = str(gameData[a]['competitions'][0]['competitors'][1]['score'])
        
        #Quick fix to change Clippers Name from LA Clippers to Los Angeles Clippers.
        if str(gameData[a]['competitions'][0]['competitors'][0]['team']['displayName']) == 'LA Clippers':
            home_Team = 'Los Angeles Clippers'
        else:
            home_Team = str(gameData[a]['competitions'][0]['competitors'][0]['team']['displayName'])
        if str(gameData[a]['competitions'][0]['competitors'][1]['team']['displayName']) == 'LA Clippers':
            away_Team = 'Los Angeles Clippers'
        else:
            away_Team = str(gameData[a]['competitions'][0]['competitors'][1]['team']['displayName'])
        
        #Appends the Game Data to the list.
        allGames.append((game_ID, game, home_Team, home_Score, away_Team, away_Score, game_Date, game_Time, game_Period, game_Status))
        a+=1
    
  
#Gets the Odds from the ODDS-API.
def oddsGetter():
    
    #Parameters for Odds Api.
    parameters = {
        "sport" : "basketball_nba",
        "region" : "uk",
        "mkt" : "h2h",
        "apiKey" : "",
    }
    
    #JSON Response.
    response = requests.get("https://api.the-odds-api.com/v3/odds/", params=parameters)
    data = response.json()['data']
    
    team0OddsInfo=[]
    team1OddsInfo=[]
    team0_odds = ''
    team1_odds = ''
    
    #Appends the odds info to a list as strings.
    for game in data:
        for site in game['sites']:
            if site['site_key'] == "paddypower":
                team0_odds = str(site['odds']['h2h'][0])
                team1_odds = str(site['odds']['h2h'][1])
        if team0_odds == '':
            team0_odds = 0
        if team1_odds == '':
            team1_odds = 0
        team0 = str(game['teams'][0])
        team1 = str(game['teams'][1])
        startTime = game['commence_time']
        gameDate = str(datetime.datetime.utcfromtimestamp(startTime).strftime('%Y-%m-%d %H:%M:%S'))[:-9]
        team0OddsInfo.append((team0, team0_odds, gameDate))
        team1OddsInfo.append((team1, team1_odds, gameDate))
    a=0
    
    #as both lists are the same length, it loops through one and Updates the tables where needed.
    while a < len(team0OddsInfo):
        query_string = 'SELECT * FROM basketbet_data.all_games WHERE Game_Date = %s'
        gameDate = (str(team0OddsInfo[a][2]),)
        mycursor.execute(query_string, gameDate)
        matchedGames = mycursor.fetchall()
        
        b=0
        while b < len(matchedGames):
            if matchedGames[b][2] == team0OddsInfo[a][0]:
                query_list = [team0OddsInfo[a][1], team1OddsInfo[a][1], matchedGames[b][0]]
                query_string = 'UPDATE all_games SET Home_Odds = %s, Away_Odds = %s WHERE (Game_ID = %s)'
                mycursor.execute(query_string, query_list)
            elif matchedGames[b][5] == team0OddsInfo[a][0]:
                query_list = [team0OddsInfo[a][1], team1OddsInfo[a][1], matchedGames[b][0]]
                query_string = 'UPDATE all_games SET Away_Odds = %s, Home_Odds = %s WHERE (Game_ID = %s)'
                mycursor.execute(query_string, query_list)
            b+=1
        a+=1
    
#For the console to show when odds were updated.
    mydb.commit()
    time = datetime.datetime.utcnow()
    print('\n' + 'ODDS UPDATE AT: ' + str(time))
    print('--------------------------------')
    print('--------------------------------')
    print(len(team0OddsInfo), "GAME ODDS inserted.")
    print('REMAINING REQUESTS:', response.headers['x-requests-remaining'])
    print('USED REQUESTS:', response.headers['x-requests-used'])
    print('--------------------------------')
    print('--------------------------------')


#Block to keep the script running then sleep for time 300 with counter set at 72 for Games every 5min | Odds every 6hr.
counter=72
startTime = time.time()
while True:
    
    #Today, Yesterday and Tomorrow.
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    tomorrow = today + datetime.timedelta(days=1)
    
    #Removing the - from the dates for the URLs, then making the URLs.
    todayShort = str(today).replace('-', '')
    yesterdayShort = str(yesterday).replace('-', '')
    tomorrowShort = str(tomorrow).replace('-', '')
    yesterdayUrl = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=" + yesterdayShort + '-' + yesterdayShort
    todayUrl = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=" + todayShort + '-' + todayShort
    tomorrowUrl = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=" + tomorrowShort + '-' + tomorrowShort
    newGetter(yesterdayUrl)
    newGetter(todayUrl)
    newGetter(tomorrowUrl)
    
    
    
    #Inserting or updating the table in MYSQL with the games.
    c=0
    updateCount=0
    newGameCount=0
    while c < len(allGames):
        query_string = 'SELECT * FROM basketbet_data.all_games WHERE Game_ID = %s'
        gameID = (str(allGames[c][0]),)
        mycursor.execute(query_string, gameID)
        
        if mycursor.fetchone():
            updateCount+=1
            query_list = [allGames[c][1], allGames[c][2], allGames[c][4], allGames[c][5], allGames[c][3], allGames[c][6], allGames[c][7], allGames[c][8], allGames[c][9], allGames[c][0]]
            query_string = 'UPDATE all_games SET Game_Name = %s, Home_Team = %s, Away_Team = %s, Away_Score = %s, Home_Score = %s, Game_Date = %s, Game_Time = %s, Game_Period = %s, Game_Status = %s WHERE (Game_ID = %s)'
            mycursor.execute(query_string, query_list)
            mydb.commit()
        else:
            newGameCount+=1
            query_string = "INSERT INTO basketbet_data.all_games (Game_ID, Game_Name, Home_Team, Home_Odds, Home_Score, Away_Team, Away_Odds, Away_Score, Game_Date, Game_Time, Game_Period, Game_Status) VALUES (%s, %s, %s, 0, %s, %s, 0, %s, %s, %s, %s, %s)"
            mycursor.execute(query_string, allGames[c])
            mydb.commit()  
        c+=1

    #Prints to console what games were updated and what new games were inserted.
    print('----------------------------------------')
    print(str(updateCount) + ' GAMES UPDATED, and ' + str(newGameCount) + ' NEW GAMES inserted.')
    print('----------------------------------------')
    allGames=[]
    
    #Counter for the Odds script.
    if counter==72:
        oddsGetter()
        counter=0
    else:
        counter+=1

    print('\n')
    time.sleep(300 - ((time.time() - startTime) % 300))