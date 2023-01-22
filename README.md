# Manipulating data from MongoDB
This project is focused on manipulating data from a MongoDB database. The project uses a json-formatted file of tweets and the MongoSpark library to insert them into a MongoDB collection called 'tweets'. The timestamp associated with each tweet is stored as a Date object and indexed to ensure fast retrieval. The geo-coordinates of tweets are also indexed properly for spatio-temporal retrieval.

## Getting Started
To get started with the project, you will need to have MongoDB and Scala installed on your machine. Additionally, you will need to import the MongoSpark library and the json file containing the tweets.

## Prerequisites
- [MongoDB](https://www.mongodb.com/)
- [Scala](https://www.scala-lang.org/)
- [MongoSpark](https://www.mongodb.com/)

## Installing
To install MongoDB and Scala, please follow the instructions provided on the respective websites. To install the MongoSpark library, you can use the following command in your Scala project:

` libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "3.4.1" `

## Running the Application
The application is run by inputting the word, radius, longitude, latitude, starting epoc time, and ending epoc time as command-line arguments. The command to run the application is as follows:

` WordFreqCalculator.scala w r lon lat start end `

- w: word to calculate its frequency
- r: radius in meters
- lon: longitude
- lat: latitude
- start: starting epoc time
- end: ending epoc time
