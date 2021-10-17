import psycopg2.extras
import configparser
from datetime import timedelta
import time

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
main_batch_size = int(config_parser.get('General', 'batch_size'))
db_name = config_parser.get('General', 'db_name')
algorithm_type = 'elbaum'
cpu_count = int(config_parser.get('General', 'cpu_count'))
failure_window_size = 24 # in number of batches (not builds) ; it should be 48 builds
execution_window_size = 48 # in number of batches (not builds) ; it should be 96 builds
reprioritize = False

if (algorithm_type not in ['elbaum']):
    print('Wrong algorithm')
    exit()

con = psycopg2.connect(database=db_name, user="postgres", password="123456", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

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


def get_running_builds(build_counter, builds, main_batch_size, start_time, run_time):
    batch_size = main_batch_size
    running_builds = builds[build_counter:build_counter + batch_size]
    build_counter += batch_size
    return build_counter, running_builds, run_time, batch_size


def get_select_query():
    select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
    return select_query


def get_new_tests(running_builds):
    running_tests = []
    select_query = get_select_query()
    for build in running_builds:
        build_id = build['build']
        filled_select_query = select_query.format(build_id)
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
    return running_tests

def test_in_failure_window(failure_window, test_name):
    if test_name in failure_window.tempset:
        return True
    for f in failure_window.list:
        if test_name in f:
            return True

def test_in_execution_window(execution_window, test_name):
    if test_name in execution_window.tempset:
        return True
    for e in execution_window.list:
        if test_name in e:
            return True

def calculate_score(failure_window, execution_window, executed_tests, test_name):
    score = 0
    # time since last failure <= w_f or time since last execution > w_e or test is new
    if(test_in_failure_window(failure_window, test_name) or not test_in_execution_window(execution_window, test_name) or test_name not in executed_tests ):
         score = 1
    return score


def rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests):
    for test in prioritized_tests:
        score = calculate_score(failure_window, execution_window, executed_tests, test[1])
        test[5] = score
    return prioritized_tests


def append_prioritized_tests(tests_to_append, prioritized_tests, failure_window, execution_window, executed_tests):
    for test in tests_to_append:
        score = calculate_score(failure_window, execution_window, executed_tests, test['test_name'])
        scoredItem = list(test)
        scoredItem.append(score)
        prioritized_tests.append(scoredItem)
    return prioritized_tests


def prioritize_tests(prioritized_tests, new_fetched_tests, failure_window, execution_window, executed_tests):
    prioritized_tests = rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests = append_prioritized_tests(new_fetched_tests, prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score


def reprioritize_tests(prioritized_tests, failure_window, failure_window_temp, execution_window, execution_window_temp, executed_tests):
    failure_window.tempset = failure_window_temp
    execution_window.tempset = execution_window_temp
    prioritized_tests = rescore_prioritized_tests(prioritized_tests, failure_window, execution_window, executed_tests)
    prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score


def initialize_counters_sets():
    failure_window_temp = set()
    execution_window_temp = set()
    return failure_window_temp, execution_window_temp


class test_information:
    def __init__(self, build, name, verdict, main_run_order, execution_time, score):
        self.build = build
        self.name = name
        self.verdict = verdict
        self.main_run_order = main_run_order
        self.execution_time = execution_time
        self.score = score


def get_first_test(tests):
    test = tests.pop(0)
    test_build = test[0]
    test_name = test[1]
    test_verdict = test[2]
    test_main_run_order = test[3]
    test_execution_time = test[4]
    test_score = test[5]
    test_info = test_information(test_build, test_name, test_verdict, test_main_run_order, test_execution_time, test_score)
    return test_info


def update_run_order_time(run_order, run_time, test_info, cpu_count, execution_window_temp, executed_tests):
    run_order += 1
    try:
        run_time += test_info.execution_time / cpu_count
    except:
        run_time += timedelta(seconds=test_info.execution_time) / cpu_count
    execution_window_temp.add(test_info.name)
    executed_tests.add(test_info.name)
    return run_order, run_time, execution_window_temp, executed_tests


def update_failures(test_name, failure_window_temp):
    # insert or update failures table
    update_query = "insert into failures values(%(testname)s, 1) " \
                   "on conflict (test_name) do " \
                   "update set fails = failures.fails + 1"
    cur.execute(update_query, {'testname': test_name})
    failure_window_temp.add(test_name)
    # notice: commit outside


def insert_runorder(algorithm_type, test_info, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test_info.build, 'test_name': test_info.name, 'verdict': test_info.verdict, 'run_order': run_order, 'run_time': run_time , 'main_run_order': test_info.main_run_order})
    # notice: commit outside


def update_sets(failure_window, failure_window_temp, execution_window, execution_window_temp):
    failure_window.add(failure_window_temp)
    execution_window.add(execution_window_temp)
    return failure_window, execution_window

class window:
    def __init__(self, size):
        self.index = 0
        self.size = size
        self.list = []
        self.tempset = set()
        for i in range(0, size):
            self.list.append(set())

    def add(self, window_temp):
        self.tempset = set()
        self.list[self.index] = window_temp # fill one of the window sets based on the size of the window and its index
        self.index = (self.index + 1) % self.size


if __name__ == '__main__':
    start = time.time()
    prioritized_tests = []
    failure_window = window(failure_window_size)
    execution_window = window(execution_window_size)
    executed_tests = set()
    run_order = 0
    run_time = timedelta()
    create_tables()
    builds, build_count = get_builds()
    start_time = builds[0]['start_time']
    build_counter = 0

    while build_counter < len(builds):
        build_counter, running_builds, run_time, batch_size = get_running_builds(build_counter, builds, main_batch_size, start_time, run_time)
        new_fetched_tests = get_new_tests(running_builds)
        # rescore remaining tests from previous runs + add new ones
        prioritize_tests(prioritized_tests, new_fetched_tests, failure_window, execution_window, executed_tests)

        failure_window_temp, execution_window_temp = initialize_counters_sets()
        for i in range(0, len(prioritized_tests)): #test in prioritized_tests:
            test_info = get_first_test(prioritized_tests)
            run_order, run_time, execution_window_temp, executed_tests = update_run_order_time(run_order, run_time, test_info, cpu_count, execution_window_temp, executed_tests)
            if test_info.verdict == False:
                update_failures(test_info.name, failure_window_temp)
                if (reprioritize == True):
                    reprioritize_tests(prioritized_tests, failure_window, failure_window_temp, execution_window, execution_window_temp, executed_tests)
                insert_runorder(algorithm_type, test_info, run_order, run_time)
                con.commit()

        failure_window, execution_window = update_sets(failure_window, failure_window_temp, execution_window, execution_window_temp)

    con.close()
    end = time.time()
    print(end - start)