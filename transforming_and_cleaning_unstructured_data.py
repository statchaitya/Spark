# TRANSFORMING and CLEANING UNSTRUCTURED DATA in Apache Spark using the Python API

# Goals: First filter the data then,
# 1) Transforming data
# 2) Cleaning unstructured data
# 3) Identifying and removing anomalies and missing values

# Filtering out a row from the data.

from pyspark import SparkContext as sc

input_path = "C:\\Github\\Spark\\Project1\\Felony.csv"

data = sc.textFile(input_path)
data.take(10) # lists first 10 rows of the data
# As of now, each row is a combined string (1 row = 1 string)

# Filter - demo
# Filtering out the header row from the data

header = data.first()
dataWoHeader = data.filter(lambda x: x!=header)

#dataWoHeader is the filtered data without the header row

#Now let us convert the string RDD into an RDD with each record representing a tuple
#Each element of a tuple will be referrenced to a column name using the header data we have filtered
#We will use the .map() operation to achieve this

dataWoHeader.map(lambda x: x.split(','))

#When you type in the above. Nothing will happen. Remember Lazy Evaluation?
#We will have to call an action on the above RDD to so that spark actually materializes it and gives us an output

dataWoHeader.map(lambda x: x.split(',')).take(5)

#This will display the first 5 rows as a lists of lists of strings

# Now let us reference each index of the individual list to a column name using the 'header' row which we had filtered)

# We will write a function for this

import csv
from io import StringIO
from collections import namedtuple

fields = header.split(",")

# Creates a class
Crime = namedtuple('Crime', fields, verbose = True)

def parse(row):
    reader = csv.reader(StringIO(row))
	row = reader.__next__()
	return Crime(*row)

crimes = dataWoHeader.map(parse)

crimes.first()

#Output:
#Crime(OBJECTID='1', Identifier='987726459', OccurrenceDate='1982-09-21 23:20:0', DayofWeek='Tuesday', 
#OccurrenceMonth='Sep', OccurrenceDay='21', OccurrenceYear='1982', OccurrenceHour='2300', CompStatMonth='Apr', 
#CompStatDay='9', CompStatYear='2015', Offense='MURDER', OffenseClassification='FELONY', Sector='', Precinct='079', 
#Borough='Brooklyn', Jurisdiction='NYPD', XCoordinate='999634', YCoordinate='190253', Latitude='40.688872153', 
#Longitude='-73.9445290319999')

crimes.first().Offense #Will give u an output of 'MURDER'

#See how we can access the column value in a namedtuple by using '.fieldname'

#Checking missing values

#Summarizing the data by 'OFFENSE' type
#Firstly printing all 'OFFENSE' column values


crimes.map(lambda x: x.Offense).foreach(print)

#print is a function in Python 3 but not in some other versions of python
#So if the above line of code throws a syntax error, define some function which prints values
#and pass it to the foreach on the respective RDD whose elements are to be printed.

crimes.map(lambda x: x.Offense).countByValue()

#OUPUT
#defaultdict(int,

#            {'BURGLARY': 10945,
#            'FELONY ASSAULT': 15056,
#             'GRAND LARCENY': 31655,
#             'GRAND LARCENY OF MOTOR VEHICLE': 5515,
#             'MURDER': 257,
#             'RAPE': 1080,
#             'ROBBERY': 12247})

#There are no missing values

#filtering the data once more, this time with a slightly complex syntax

crimesFiltered = crimes.filter(lambda x: not (x.Offense=="NA" or x.OccurrenceYear==''))\
                     .filter(lambda x: int(x.OccurrenceYear) >= 2012)


#Identifying and filtering anomalies
#Checking for corrupt records
#Ex: We are dealing with data related to NY. If the location turns up to be 'Canada', then there is something wrong
#Checking for anomalies such as these by checking the minimum and the maximum LATITUDE and LONGITUDE

crimesFiltered.map(lambda x: (float(x.Latitude), float(x.Longitude))).reduce(lambda x,y: (min(x[0],y[0]), min(x[1], y[1])))
crimesFiltered.map(lambda x: (float(x.Latitude), float(x.Longitude))).reduce(lambda x,y: (max(x[0],y[0]), max(x[1], y[1])))

#Now let us summarize and visualize crimes in NYC
#Let us look at the trend of crimes by year

crimesFiltered.map(lambda x: x.OccurrenceYear).countByValue()

#Visualizing the density of BURGLARY crimes on google maps using the 'gmap' library in python

#Importing the package and preparing the base of our map which will be centered on NYC
import gmplot
gmap = gmplot.GoogleMapPlotter(37.428, -122.145, 16).from_geocode("New York City")

#Collecting latitudes in a list
#Note that we are only looking at the Offense 'BURGLARY' occurred in year 2015
b_lats = crimesFiltered.filter(lambda x: x.Offense=="BURGLARY" and x.OccurrenceYear == "2015").map(lambda x: float(x.Latitude)).collect()
#Collecting longitudes in a list
b_longs = crimesFiltered.filter(lambda x: x.Offense=="BURGLARY" and x.OccurrenceYear == "2015").map(lambda x: float(x.Longitude)).collect()

#Plotting using the scatter method
gmap.scatter(b_lats, b_longs, '#4440C8', size = 40, marker = False)
#Display it
gmap.draw("mymap.html")
                   

