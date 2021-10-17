import psycopg2.extras

db_name = 'chromium'
con = psycopg2.connect(database=db_name, user='postgres', password='secret', host='localhost', port='5432')
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
outlier_count = 27

def get_outlier_builds():
        query = "select build_id, count(*) failures from " \
                "trybot_test_results where status in ('FAIL', 'CRASH', 'ABORT') and final_status = 'UNEXPECTED'" \
                " and build_start_t > '2021-01-01' and build_start_t < '2021-02-01' and duration is not NULL" \
                " group by build_id having count(*) > {}".format(outlier_count)

        cur.execute(query)
        builds = cur.fetchall()
        builds_outlier = '(%s)' % ', '.join(["'%s'" % item[0] for item in builds])
        return builds_outlier

if __name__ == '__main__':
        builds_outlier = get_outlier_builds()
        cur.execute('delete from tests where build in {}'.format(builds_outlier))
        cur.execute('delete from tests_unexpected where build in {}'.format(builds_outlier))
        con.commit()
