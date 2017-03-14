trafficPath = 'C:\\Pluralsight\\Dodgers.data'

gamesPath = 'C:\\Pluralsight\\Dodgers.events'

traffic = sc.textFile(trafficPath)

games = sc.textFile(gamesPath)

#traffic.take(10)
#games.take(10)

''' For a pair RDD we need each record to be a tuple with two elements '''

from datetime import datetime
import csv
from io import StringIO

''' Defining a function that takes in a string and throws out two elements as a tuple '''

def parseTraffic(row):
    #representation of date format being used in a datafile for correct string to date parsing
    DATE_FMT = "%m/%d/%Y %H:%M"
	row = row.split(",")
	#parsing a string into a date object
	row[0] = datetime.strptime(row[0], DATE_FMT)
	row[1] = int(row[1])
	return (row[0], row[1])
	
#Creating a pair RDD
trafficParsed = traffic.map(parseTraffic)

#trafficParsed.take(10)

''' reduceByKey operation on pair RDDs lets us combine values of the same key in a particular way '''

''' Looking at traffic trend '''
''' We have TIME details in column 1 and no. of cars which passed at that time in column 2 '''
''' Let us now sum the total no. of cars passed by in a day using the DAY as a key '''

#Extracting date from timestamp.
dailyTrend = trafficParsed.map(lambda x: (x[0].date(),x[1])).reduceByKey(lambda x,y: x+y)

#dailyTrend.take(10)
#sorting values
dailyTrendSortedDesc = dailyTrend.sortBy(lambda x: -1*x[1])
#dailyTrendSortedDesc.take(10)
dailyTrendSortedAsce = dailyTrend.sortBy(lambda x: x[1])
#dailyTrendSortedAsce.take(10)

''' Now we have top 10 days having least and the most traffic '''
''' It will be intersting to hypothesize if the days having highest traffic are game days '''
''' For this we will use games data by merging the traffic and the games data together '''

def parseGames(row):
    #representation of date format being used in a datafile for correct string to date parsing
    DATE_FMT = "%m/%d/%y"
	row = row.split(",")
	#parsing a string into a date object
	row[0] = datetime.strptime(row[0], DATE_FMT).date()
	#row[4] contains the OPPONENT TEAM
	return (row[0], row[4])

#Using a leftOuterJoin to also keep the Dates when there was No game (Opponent will be none for these dates)
dailyTrendCombined = dailyTrend.leftOuterJoin(gamesParsed)

#dailyTrendCombined.take(10)

def checkGameDay(row):
    if row[1][1] == None
	    return (row[0], row[1][1], "Regular Day", row[1][0])
		#This will return Date; Opponent; Type of Day; Cars)
	else:
	    return (row[0], row[1][1], "Game Day", row[1][0])
		
dailyTrendByGames = dailyTrendCombined.map(checkGameDay)

#dailyTrendByGames.take(10)
#sorting

dailyTrendByGamesSorted = dailyTrendByGames.sortBy(lambda x: -1*x[3])
#dailyTrendByGamesSorted.take(10)

''' OUTPUT '''
#[(datetime.date(2005, 7, 28), 'Cincinnati', 'Game Day', 7661),
# (datetime.date(2005, 7, 29), 'St. Louis', 'Game Day', 7499),
# (datetime.date(2005, 8, 12), 'NY Mets', 'Game Day', 7287),
# (datetime.date(2005, 7, 27), 'Cincinnati', 'Game Day', 7238),
# (datetime.date(2005, 9, 23), 'Pittsburgh', 'Game Day', 7175),
# (datetime.date(2005, 7, 26), 'Cincinnati', 'Game Day', 7163),
# (datetime.date(2005, 5, 20), 'LA Angels', 'Game Day', 7119),
# (datetime.date(2005, 8, 11), 'Philadelphia', 'Game Day', 7110),
# (datetime.date(2005, 9, 8), None, 'Regular Day', 7107),
# (datetime.date(2005, 9, 7), 'San Francisco', 'Game Day', 7082)]

''' All but one top results returned GAME DAYS. This definitely shows that our hypothesis that
traffic is the most on GameDays is acceptable '''

''' Now let us answer: What was the average traffic on a GAME DAY vs. REGULAR DAY '''

#Key = Game Day or No Game Day
AvgPerKey = dailyTrendByGames.map(lambda x: (x[2],x[3]))\
                   .combineByKey(lambda value: (value,1),\
				   lambda acc, value: (acc[0]+value, acc[1]+1),\
				   lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))\
				   .mapValues(lambda x:x[0]/x[1])\
				   .collect()

''' OUTPUT '''
#[('Regular Day', 5411.329787234043), ('Game Day', 5948.604938271605)]

''' We can see that the average no. of cars passing by on a Game Day are more than a Regular Day '''


