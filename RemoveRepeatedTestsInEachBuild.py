import psycopg2.extras
import argparse
import sys

db_name = 'chromium'
pre_post = 'post'
table = 'tests'
con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_builds(table):
    cur.execute("select build, min(start_time) start_time from {} ".format(table) + " group by build, start_time order by min(start_time) asc")
    builds = cur.fetchall()
    build_count = cur.rowcount
    return builds, build_count


def get_select_query(table):
    orderby_column = "start_time"
    select_query = "select build, test_id, test_name from {}".format(table) + " where build = '{}' order by " + orderby_column
    return select_query


def get_running_tests(running_builds, table):
    running_tests = []
    select_query = get_select_query(table)
    for build in running_builds:
        filled_select_query = select_query.format(build['build'])
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
    return running_tests

def process_builds(running_builds, table):
    running_tests = get_running_tests(running_builds, table)
    test_list = []
    for test in running_tests:
        if test['test_name'] in test_list:
            query = "delete from {} where build = '{}' and test_id = '{}'".format(table, test['build'], test['test_id'])
            cur.execute(query)
        else:
            test_list.append(test['test_name'])
    con.commit()

def remove_repeated_fails_in_builds(table):
    builds, build_count = get_builds(table)
    for l in range(0, build_count):
        running_build = builds[l]
        process_builds([running_build], table)
    con.close()

def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", type=str, default='tests')
    args = parser.parse_args(args)
    remove_repeated_fails_in_builds(args.table)

if __name__ == '__main__':
    main(sys.argv[1:])
