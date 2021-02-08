#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assignment1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data1.txt'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

import psycopg2
import traceback
import Interface1 as MyAssignment

if __name__ == '__main__':
    try:
        MyAssignment.createDB(DATABASE_NAME)

        with MyAssignment.getOpenConnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

           # MyAssignment.deleteAllPublicTables(conn)
            MyAssignment.loadRatings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            
            MyAssignment.loadRatings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            # Doing Range Partition
            print("Doing the Range Partitions")
            MyAssignment.rangePartition('ratings', 5, conn)

            # Doing Round Robin Partition
            print("Doing the Round Robin Partitions")
            MyAssignment.roundRobinPartition('ratings', 5, conn)

            # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
            MyAssignment.deleteTables('ratings', conn)

            # Calling rangeQuery
            print("Performing Range Query")
            MyAssignment.rangeQuery(1.5, 3.5, conn, "./rangeResult.txt")
            #MyAssignment.rangeQuery(1,4,conn, "./rangeResult.txt")

            # Calling pointQuery
            print("Performing Point Query")
            MyAssignment.pointQuery(4.5, conn, "./pointResult.txt")
            #MyAssignment.pointQuery('2,conn, "./pointResult.txt")

           

    except Exception as detail:
        traceback.print_exc()
