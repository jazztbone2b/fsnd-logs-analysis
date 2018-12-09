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
        select articles.title, substring(path, 10) as url, count(*) as views
        from log
        join articles on substring(path, 10)=articles.slug
        where substring(path, 10) > ''
        group by articles.title, path
        order by views desc limit 3;
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
        select authors.name, count(authors.name) as reads
        from articles
        inner join authors on articles.author=authors.id
        inner join log on articles.slug=substring(log.path, 10)
        where substring(log.path, 10) > ''
        group by name
        order by reads desc;
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
        select *,
        round(((bad_reqs.total_bad_reqs*1.0) / (total_reqs.total)), 3)
        as percent_failed
        from (
            select
            to_char(log.time, 'YYYY-MM-DD') as date,
            count(status) as total
            from log
            group by date
            order by date asc
        ) as total_reqs
        join (
            select
            to_char(log.time, 'YYYY-MM-DD') as date,
            count(status) as total_bad_reqs
            from log
            where status != '200 OK'
            group by date
            order by date asc
        ) as bad_reqs
        on total_reqs.date = bad_reqs.date
        where
        (round(((bad_reqs.total_bad_reqs*1.0) / total_reqs.total), 3) > 0.01);
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
