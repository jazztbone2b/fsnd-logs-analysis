# This Python file uses the following encoding: utf-8
# Coded by Collin Banks for the Udacity Full Stack Nanodegree

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

#3. On which days did more than 1% of requests lead to errors? The log table includes a column status that indicates the HTTP status code that the news site sent to the user's browser. 

    #Example:

    #July 29, 2016 — 2.5% errors

import psycopg2
import calendar

def most_popular_articles():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute("select articles.title, substring(path, 10) as url, count(*) as views from log join articles on substring(path, 10)=articles.slug where substring(path, 10) > '' group by articles.title, path order by views desc limit 3;")
    most_read_articles = c.fetchall()
    db.close()
    print("Top 3 Articles of All Time:")
    for x in most_read_articles:
        print(x[0] + ' - ' + str(x[2]) + ' views')
    return most_read_articles

most_popular_articles()

print(' ')

def most_popular_author():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute("select authors.name, count(authors.name) as reads from articles inner join authors on articles.author=authors.id inner join log on articles.slug=substring(log.path, 10) where substring(log.path, 10) > '' group by name order by reads desc;")
    total_read_articles = c.fetchall()
    db.close()
    print("Most Popular Author:")
    for x in total_read_articles:
        print(x[0] + ' - ' + str(x[1]) + ' views')
    return total_read_articles

most_popular_author()

print(' ')

def errors():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute("select *, round(((bad_reqs.total_bad_reqs*1.0) / (total_reqs.total)), 3) as percent_failed from (select to_char(log.time, 'YYYY-MM-DD') as date, count(status) as total from log group by date order by date asc) as total_reqs join (select to_char(log.time, 'YYYY-MM-DD') as date, count(status) as total_bad_reqs from log where status = '404 NOT FOUND' group by date order by date asc) as bad_reqs on total_reqs.date = bad_reqs.date where (round(((bad_reqs.total_bad_reqs*1.0) / total_reqs.total), 3) > 0.01);")
    most_errors = c.fetchall()
    db.close()
    print("Days with more than 1 percent of errors:")
    for x in most_errors:
        to_date = x[0]
        year = to_date[0:4]
        to_month = int(to_date[5:7])
        day = to_date[8:10]
        named_month = calendar.month_name[to_month]
        percent_errors = str(round(x[4] * 100, 2))
        print(named_month + ' ' + day + ', ' + year + ' - ' + percent_errors + '% errors')
    return most_errors

errors()