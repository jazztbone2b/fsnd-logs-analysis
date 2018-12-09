#!/usr/bin/env python
# This Python file uses the following encoding: utf-8
# Coded by Collin Banks for the Udacity Full Stack Nanodegree

import psycopg2
import calendar

print(' ')


def most_popular_articles():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute("""
        SELECT articles.title, substring(path, 10) as url, count(*) as views
        FROM log
        JOIN articles ON substring(path, 10)=articles.slug
        WHERE substring(path, 10) > ''
        GROUP BY articles.title, path
        ORDER BY views desc
        LIMIT 3;
        """)
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
    c.execute("""
        SELECT authors.name, count(authors.name) as reads
        FROM articles
        INNER JOIN authors ON articles.author=authors.id
        INNER JOIN log ON articles.slug=substring(log.path, 10)
        WHERE substring(log.path, 10) > ''
        GROUP BY name
        ORDER BY reads desc;
        """)
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
    c.execute("""
        SELECT *,
        ROUND(((bad_reqs.total_bad_reqs*1.0) / (total_reqs.total)), 3)
        AS percent_failed
        FROM (
            select
            to_char(log.time, 'YYYY-MM-DD') as date,
            count(status) as total
            from log
            group by date
            order by date asc
        ) AS total_reqs
        JOIN (
            SELECT
            to_char(log.time, 'YYYY-MM-DD') as date,
            count(status) as total_bad_reqs
            FROM log
            WHERE status != '200 OK'
            GROUP BY date
            ORDER BY date asc
        ) as bad_reqs
        ON total_reqs.date = bad_reqs.date
        WHERE
        (ROUND(((bad_reqs.total_bad_reqs*1.0) / total_reqs.total), 3) > 0.01);
        """)
    most_errors = c.fetchall()
    db.close()
    print("Days with more than 1 percent of errors:")
    for x in most_errors:
        to_date = x[0]
        year = to_date[0:4]
        to_month = int(to_date[5:7])
        day = to_date[8:10]
        month = calendar.month_name[to_month]
        perc_err = str(round(x[4] * 100, 2))

        print(month + ' ' + day + ', ' + year + ' - ' + perc_err + '% errors')
    return most_errors


errors()
