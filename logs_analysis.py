# This Python file uses the following encoding: utf-8

#Create 3 different query functions

#1. What are the most popular three articles of all time? Which articles have been accessed the most? Present this information as a sorted list with the most popular article at the top.

    #Example:

    #"Princess Shellfish Marries Prince Handsome" — 1201 views
    #"Baltimore Ravens Defeat Rhode Island Shoggoths" — 915 views
    #"Political Scandal Ends In Political Scandal" — 553 views

#2. Who are the most popular article authors of all time? That is, when you sum up all of the articles each author has written, which authors get the most page views? Present this as a sorted list with the most popular author at the top.

    #Example:

    #Ursula La Multa — 2304 views
    #Rudolf von Treppenwitz — 1985 views
    #Markoff Chaney — 1723 views
    #Anonymous Contributor — 1023 views

#3. On which days did more than 1% of requests lead to errors? The log table includes a column status that indicates the HTTP status code that the news site sent to the user's browser. (Refer to this lesson for more information about the idea of HTTP status codes.)

    #Example:

    #July 29, 2016 — 2.5% errors
import psycopg2

print('hey there')

#db_name = "news"

def most_popular_articles():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute("select articles.title, substring(path, 10) as url, count(*) as views from log join articles on substring(path, 10)=articles.slug where substring(path, 10) > '' group by articles.title, path order by views desc limit 3;")
    most_read_articles = c.fetchall()
    db.close()
    print(most_read_articles)
    print('"' + most_read_articles[0][0] + '"' + ' - ' + str(most_read_articles[0][2]) + ' views')
    print('"' + most_read_articles[1][0] + '"' + ' - ' + str(most_read_articles[1][2]) + ' views')
    print('"' + most_read_articles[2][0] + '"' + ' - ' + str(most_read_articles[2][2]) + ' views')
    return most_read_articles

most_popular_articles()

#def most_popular_authors():

#def least_amount_of_errors():