import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    o=openconnection
    cur = o.cursor()
    
    query_createTable = "Create table "+ratingstablename+"(userid integer, sep1 char, movieid integer, sep2 char, rating float, sep3 char, timestamp bigint);"
    cur.execute(query_createTable)
    openconnection.commit()
    
    f = open(ratingsfilepath, 'r')
    cur.copy_from(f,ratingstablename,sep=':')

    #Drop the extra columns with separators and timestamp from ratings table
    query_dropColumns="Alter table " + ratingstablename + " drop column sep1, drop column sep2, drop column sep3, drop column timestamp;"
    cur.execute(query_dropColumns)
    
    cur.close()
    o.commit()
 

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    o=openconnection
    cur = o.cursor()
    
    partition_range = 5.0/ numberofpartitions
    
    for i in range(numberofpartitions):
        min_rating = i * partition_range
        max_rating = min_rating + partition_range
        partition_table_name = RANGE_TABLE_PREFIX + str(i)
        
        cur.execute("Create table " + partition_table_name + " (userid integer, movieid integer, rating float);")
        
        if i==0:
          cur.execute("Insert into " + partition_table_name + " (userid, movieid, rating) Select userid, movieid, rating from " + ratingstablename + 
          " where rating >= " + str(min_rating) + " and rating <= " + str(max_rating) + ";")
        else:
          cur.execute("Insert into " + partition_table_name + " (userid, movieid, rating) Select userid, movieid, rating from " + ratingstablename + 
          " where rating >" + str(min_rating) + " and rating <=" + str(max_rating) + ";")
    
    cur.close()
    o.commit()
    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    o=openconnection
    cur = o.cursor()
    
    # Add extra column to rating table to maintain row index
    cur.execute("Alter table " + ratingstablename + " add rowno serial")

    for i in range(numberofpartitions):
        partition_table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("Create table " + partition_table_name + " (userid integer, movieid integer, rating float)")
        cur.execute("Insert into " + partition_table_name + " (userid, movieid, rating)"
        "Select userid, movieid, rating from " + ratingstablename +
        " where (rowno-1) % " + str(numberofpartitions)+ " = " + str(i) + ";")

    # Drop the extra row index column
    cur.execute("Alter table " + ratingstablename + " DROP COLUMN rowno")
    
    cur.close()
    o.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    o=openconnection
    cur=o.cursor()

    no_of_partitions=get_partitions_count(RANGE_TABLE_PREFIX, openconnection)
    partition_range=5.0/no_of_partitions

    for i in range(no_of_partitions):
        min=i*partition_range
        max=min+partition_range

        if i==0:
           if(rating>=min and rating<=max): 
                partition_table=RANGE_TABLE_PREFIX+str(i)
                cur.execute("Insert into " + ratingstablename + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))  
                cur.execute("Insert into " + partition_table + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))
                break

        else:
            if(rating>min and rating<=max):
                partition_table=RANGE_TABLE_PREFIX+str(i)
                cur.execute("Insert into " + ratingstablename + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))  
                cur.execute("Insert into " + partition_table + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))
                break

    cur.close()
    o.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    o=openconnection
    cur=o.cursor()  
    
    no_of_partitions=get_partitions_count(RROBIN_TABLE_PREFIX, openconnection)      
    cur.execute("Select count(*) from " + ratingstablename + ";")
    total_rows = cur.fetchone()[0]

    table_no = (total_rows) % no_of_partitions
    
    partition_table = RROBIN_TABLE_PREFIX + str(table_no)
    cur.execute("Insert into " + partition_table + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))
    cur.execute("Insert into " + ratingstablename + "(userid, movieid, rating) VALUES (%s, %s, %s)", (userid ,itemid ,rating))
    
    cur.close()
    o.commit()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    o=openconnection
    cur=o.cursor() 
    
    # check and remove if output file exists
    if(os.path.exists(outputPath)):
        os.remove(outputPath)

    file=open(outputPath,'a')

    #range ratings partition range query
    no_of_partitions=get_partitions_count(RANGE_TABLE_PREFIX, openconnection)    
    for i in range(no_of_partitions):
        partition_table=RANGE_TABLE_PREFIX+str(i)
        query="Select * from " + partition_table + " where rating >=" + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue)
        cur.execute(query) 
        r = cur.fetchone()
        while r:
            result = partition_table + "," + str(r[0])+ "," + str(r[1])+ "," + str(r[2])+ "\n"
            file.write(result)
            r = cur.fetchone()
    
    #round robin partition range query
    no_of_partitions=get_partitions_count(RROBIN_TABLE_PREFIX, openconnection)
    for i in range(no_of_partitions):
        partition_table=RROBIN_TABLE_PREFIX+str(i)
        query="Select * from " + partition_table + " where rating >=" + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue)
        cur.execute(query) 
        r = cur.fetchone()
        while r:
            result = partition_table + "," + str(r[0])+ "," + str(r[1])+ "," + str(r[2])+ "\n"
            file.write(result)
            r = cur.fetchone()       
           
    file.close()
    cur.close()
    o.commit()


def pointQuery(ratingValue, openconnection, outputPath):  
    o=openconnection
    cur=openconnection.cursor() 

    # check and remove if output file exists
    if(os.path.exists(outputPath)):
        os.remove(outputPath)

    file=open(outputPath,'a')
 
    #range ratings partition point query
    no_of_partitions=get_partitions_count(RANGE_TABLE_PREFIX, openconnection)
    for i in range(no_of_partitions):
        partition_table=RANGE_TABLE_PREFIX+str(i)
        query="Select * from " + partition_table + " where rating = " + str(ratingValue) + ";"
        cur.execute(query) 
        r = cur.fetchone()
        while r:
            result = partition_table + "," + str(r[0])+ "," + str(r[1])+ "," + str(r[2])+ "\n"
            file.write(result)
            r = cur.fetchone()

    #roud robin partition point query
    no_of_partitions=get_partitions_count(RROBIN_TABLE_PREFIX, openconnection)
    for i in range(no_of_partitions):
        partition_table=RROBIN_TABLE_PREFIX+str(i)
        query="Select * from " + partition_table + " where rating = " + str(ratingValue) + ";"
        cur.execute(query) 
        r = cur.fetchone()
        while r:
            result = partition_table + "," + str(r[0])+ "," + str(r[1])+ "," + str(r[2])+ "\n"
            file.write(result)
            r = cur.fetchone()

    file.close()
    cur.close()
    o.commit()

def get_partitions_count(prefix, openconnection):
    o=openconnection
    cur = o.cursor()
    cur.execute("Select COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(prefix))
    count = int(cur.fetchone()[0])
    cur.close()
    o.commit()
    return count
   

def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
