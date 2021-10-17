# prioritization and re-prioritization based on kim and porter idea
# using previous failures and scoring
import psycopg2.extras
import configparser
from datetime import timedelta
import time

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
db_name = config_parser.get('General', 'db_name')
algorithm_type = 'kimporter'
recent_coef = float(config_parser.get('General', 'recent_coefficient')) # alpha
reprioritize = False
batch_size = 1
cpu_count = int(config_parser.get('General', 'cpu_count'))

if(algorithm_type not in ['kimporter']):
  print('Wrong algorithm')
  exit()

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
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
              "primary key (test_name))")
  con.commit()

def get_builds():
    cur.execute("select build, min(start_time) start_time from tests_unexpected group by build, start_time order by min(run_order) asc")
    builds = cur.fetchall()
    build_count = cur.rowcount
    return builds, build_count


def get_select_query():
  select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
  return select_query


def get_new_tests(running_builds):
  running_tests = []
  select_query = get_select_query()
  for build in running_builds:
      filled_select_query = select_query.format(build['build'])
      cur.execute(filled_select_query)
      new_fetched_tests = cur.fetchall()
      running_tests.extend(new_fetched_tests)
  return running_tests


def get_test_previous_failures_db(builds, test_name):  # returns 1 for any failures
  builds = '(%s)' % ', '.join(["'%s'" % item[0] for item in builds])
  query = "select count(*) from {0} where build in {1} and test_name = '{2}' and verdict = false".format(
      algorithm_type, builds, test_name)
  cur.execute(query)
  failure_count = cur.fetchall()[0][0]
  if failure_count > 0:
      return 1
  return 0


def get_test_previous_failures(test_name, previous_batch_test_failures):
  if test_name in previous_batch_test_failures:
      return 1
  else:
      return 0


def calculate_score(build_start, test_name, last_test_score, previous_batch_test_failures):
  previous_build_start = build_start - batch_size
  if build_start < 0 or previous_build_start < 0:
      return 0
  return recent_coef * get_test_previous_failures(test_name, previous_batch_test_failures) + \
         (1-recent_coef) * last_test_score


def append_prioritized_tests(tests_to_append, prioritized_tests, build_start, last_test_scores, previous_batch_test_failures):
  calculated_tests = {} # to prevent recalculation for the same test_name
  for test in tests_to_append:
      test_name = test['test_name']
      if test_name not in last_test_scores.keys():
          last_test_scores[test_name] = 0
      if test_name in calculated_tests:
          score = calculated_tests[test_name]
      else:
          score = calculate_score(build_start, test_name, last_test_scores[test_name], previous_batch_test_failures)
          last_test_scores[test_name] = score
          calculated_tests[test_name] = score
      scoredItem = list(test)
      scoredItem.append(score)
      prioritized_tests.append(scoredItem)
  return prioritized_tests, last_test_scores


def rescore_prioritized_tests(prioritized_tests, build_start, last_test_scores, previous_batch_test_failures):
  calculated_tests = {} # to prevent recalculation for the same test_name
  for test in prioritized_tests:
      test_name = test[1]
      if test_name not in last_test_scores:
          last_test_scores[test_name] = 0
      if test_name in calculated_tests:
          score = calculated_tests[test_name]
      else:
          score = calculate_score(build_start, test_name, last_test_scores[test_name], previous_batch_test_failures)
          last_test_scores[test_name] = score
          calculated_tests[test_name] = score
      test[5] = score
  return prioritized_tests, last_test_scores


def update_failures(test_name, build_test_failures):
  if test_name in build_test_failures:
      build_test_failures[test_name] += 1
  else:
      build_test_failures[test_name] = 1
  # insert or update failures table
  update = "insert into failures values(%(testname)s, 1) " \
           "on conflict (test_name) do " \
           "update set fails = failures.fails + 1"
  cur.execute(update, {'testname': test_name})

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
  return test_info, test


def update_run_order_time(run_order, run_time, execution_time, cpu_count):
  run_order += 1
  try:
    run_time += execution_time / cpu_count
  except:
    run_time += timedelta(seconds=execution_time) / cpu_count
  return run_order, run_time


def insert_runorder(algorithm_type, test_info, run_order, run_time):
  insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                     " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
      algorithm_type)
  cur.execute(insert_run_order,
              {'build': test_info.build, 'test_name': test_info.name, 'verdict': test_info.verdict, 'run_order': run_order, 'run_time': run_time , 'main_run_order': test_info.main_run_order})
  # notice: commit outside


if __name__ == '__main__':
    start = time.time()
    create_tables()
    builds, build_count = get_builds()
    start_time = builds[0]['start_time']
    last_test_scores = {}
    run_order = 0
    run_time = timedelta()
    prioritized_tests = []
    batch_test_failures = {}
    build_counter = 0

    for l in range(0, build_count):
      running_build = builds[l]
      build_arrival = running_build['start_time'] - start_time
      new_fetched_tests = get_new_tests([running_build])
      prioritized_tests, last_test_scores = append_prioritized_tests(new_fetched_tests, prioritized_tests, l, last_test_scores, batch_test_failures)
      prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score

      batch_test_failures = {}
      test_list_len = len(prioritized_tests)
      for i in range(0, test_list_len):  # test in prioritized_tests:
          test_info, test = get_first_test(prioritized_tests)
          run_order, run_time = update_run_order_time(run_order, run_time, test_info.execution_time, cpu_count)

          if test_info.verdict == False:
              update_failures(test_info.name, batch_test_failures)
              # Reprioritize
              if(reprioritize == True):
                  prioritized_tests, last_test_scores = rescore_prioritized_tests(prioritized_tests, l, last_test_scores, batch_test_failures)
                  prioritized_tests.sort(key=lambda x: x[5], reverse=True)  # prioritize based on score
              insert_runorder(algorithm_type, test_info, run_order, run_time)
              con.commit()
    con.close()
    end = time.time()
    print(end - start)
