import psycopg2.extras
import configparser
from datetime import timedelta

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
db_name = config_parser.get('General', 'db_name')
algorithm_type = 'fifo'
cpu_count = int(config_parser.get('General', 'cpu_count'))

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

if(algorithm_type not in ['fifo']):
  print('Wrong algorithm')
  exit()

def create_tables():
    cur.execute("drop table if exists {}".format(algorithm_type))
    cur.execute("create table {}("
                "build text,"
                "test_name text,"
                "verdict boolean,"
                "run_order int,"
                "run_time interval,"
                "main_run_order int)".format(algorithm_type))
    con.commit()
    cur.execute("drop table if exists failures")
    cur.execute("create table failures("
                "test_name text,"
                "fails int default 0,"
                "primary key (test_name)")
    con.commit()


def get_builds():
    cur.execute("select build, min(start_time) start_time from tests_unexpected group by build, start_time order by min(run_order) asc")
    builds = cur.fetchall()
    build_count = cur.rowcount
    return builds, build_count


def get_select_query():
    select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
    return select_query


def get_running_tests(running_builds):
    running_tests = []
    select_query = get_select_query()
    for build in running_builds:
        filled_select_query = select_query.format(build['build'])
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
    return running_tests


def update_failures(test_name):
    # insert or update failures table
    update_query = "insert into failures values(%(testname)s, 1) " \
                   "on conflict (test_name) do " \
                   "update set fails = failures.fails + 1"
    cur.execute(update_query, {'testname': test_name})


def insert_runorder(algorithm_type, test, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test['build'], 'test_name': test['test_name'], 'verdict': test['verdict'],
                 'run_order': run_order, 'run_time': run_time, 'main_run_order': test['run_order']})
    # notice: commit outside


def process_builds(running_builds, cpu_count, run_order, run_time):
    running_tests = get_running_tests(running_builds)
    for test in running_tests:
        run_order += 1
        run_time += test['execution_time'] / cpu_count
        if test['verdict'] == False:
            update_failures(test['test_name'])
            insert_runorder(algorithm_type, test, run_order, run_time)
    con.commit()
    return run_order, run_time


if __name__ == '__main__':
    create_tables()
    builds, build_count = get_builds()
    start_time = builds[0]['start_time']
    run_order = 0
    run_time = timedelta()

    for l in range(0, build_count):
        running_build = builds[l]
        build_arrival = running_build['start_time'] - start_time
        run_order, run_time = process_builds([running_build], cpu_count, run_order, run_time)

    con.close()
