# FizzBuzz-Connector

Presto Connectors are part of the Plugin abstraction for Presto.  An important part of the plugin abstraction is the concept of Connector.  A Connector is the data source and data destination for all Presto queries.  Presto provides an SPI that allows you to build custom connectors.

FizzBuzz is a popular programming trivia.  It is described generically in this Wikipedia article.  The gist for FizzBuzz is you take an integer and check if it’s divisible by 3 or 5.  If it’s divisible by 3, you print “Fizz”, and if it’s divisible by 5, you print “Buzz”.  If it’s divisible by both, you print “FizzBuzz”.

This Presto connector returns a table of data with two columns Id and FizzBuzz. The Id column consists of numbers ranging from 1 to 10000 and the FizzBuzz column consists of the corresponding "Fizz" entries, "Buzz" entries, "FizzBuzz" entries or the number itself.

# Requirements:

The connector must return the above table of data.  It may generate this data completely in memory (i.e., no need to interact with an external system).
The connector does not need to write any data.
There need only be one type of split.  Split enumeration can simply return a single split, which returns the above table of data.
As a result of implementing this connector, we can show a query which returns the above table.  Furthermore, we can filter the query to show all fizz entries (divisible by 3), all buzz entries (divisible by 5), and all fizzbuzz entries (divisible by both).

# Set up Presto environment using Docker:

Follow the instructions in this link: 
https://hub.docker.com/r/ahanaio/prestodb-sandbox

# Steps to be followed after setting up the Presto environment:

Run the following queries:

Creating Presto catalog:
CREATE CATALOG fizzbuzz;

Creating Presto schema:
CREATE SCHEMA fizzbuzz.default;

Creating FizzBuzz table:
CREATE TABLE fizzbuzz.default.fizzbuzz (
    id INTEGER,
    FizzBuzz STRING
);

# Executing the FizzBuzz program:

java -cp presto-client-latest.jar com.facebook.presto.client.QueryRunner fizzbuzz:default

This starts a Presto client that connects to the Presto server. The SQL queries to interact with the FizzBuzz table are as follows.

# Retrieving results from FizzBuzz table:

Printing the entire FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz;

Printing all Fizz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'Fizz';

Printing all Buzz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'Buzz';

Printing all FizzBuzz entries in the FizzBuzz table:
SELECT * FROM fizzbuzz.default.fizzbuzz WHERE FizzBuzz = 'FizzBuzz';
