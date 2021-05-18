
#Assignment2 Interface

import psycopg2
import os
import sys
import threading
# Donot close the connection inside this file i.e. do not perform openconnection.close()

##########################################  Parallel Sort ##################################################################

#Function to Parallel sort the InputTable on SortingColumnName and store results in OutputTable
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    o = openconnection
    cur = o.cursor()

    #Get min and max value of Sorting Column
    max_col = getMaxValue(openconnection, SortingColumnName, InputTable)
    min_col = getMinValue(openconnection, SortingColumnName, InputTable)

    no_threads = 5
    
    # range of partition
    diff = float((max_col - min_col) / no_threads)

    tableName = "range_part"

    #create threads list
    threadList = [0,0,0,0,0]
    for i in range(no_threads):
        min_range = min_col + i * diff
        max_range = min_range + diff
        threadList[i] = threading.Thread(target=range_sort, args=(InputTable, tableName, SortingColumnName, min_range, max_range, i, openconnection))
        threadList[i].start()

    #wait for all threads to finish task
    for i in range(no_threads):
        threadList[i].join()
    
    #Create output table schema similar to Input table
    cur.execute("Drop table if exists " + OutputTable + ";")
    cur.execute("Create table " + OutputTable + " ( LIKE " + InputTable + " including all );")

    #Insert all sorted tables to output table
    for i in range(no_threads):
        cur.execute("Insert into " + OutputTable +" Select * from " + tableName + str(i) + ";")
    
    o.commit()


#Function to create partition sorted by SortingColumnName
def range_sort(InputTable, table_prefix, SortingColumnName, min_range, max_range, i, openconnection):

    o = openconnection	
    cur = o.cursor()

    cur.execute("Drop table if exists  " + table_prefix + str(i) + ";")
    cur.execute("Create table " + table_prefix + str(i) + " ( LIKE " + InputTable + " including all);")

    if i==0:
        cur.execute("Insert into "  + table_prefix + str(i) +" Select * from " + InputTable + " where " + SortingColumnName + " >= "+ str(min_range) + 
                   " and " + SortingColumnName + " <= " + str(max_range) + " ORDER BY " + SortingColumnName + " ASC;")
    else:
        cur.execute("Insert into "+ table_prefix + str(i) +" Select * from  " + InputTable + " where " + SortingColumnName + " > "+ str(min_range) + 
                   " and " + SortingColumnName + " <= " + str(max_range) + " ORDER BY " + SortingColumnName + " ASC;")
        
    o.commit()


##########################################  Parallel Join  ##################################################################

#Function to Parallel join InputTable1 and InputTable2 on Table1JoinColumn and Table2JoinColumn and store results in OutputTable
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    o = openconnection
    cur = o.cursor()
    
    #Get max and min values of Table1JoinColumn
    max_col1 = getMaxValue(openconnection, Table1JoinColumn, InputTable1)
    min_col1 = getMinValue(openconnection, Table1JoinColumn, InputTable1)

    #Get max and min values of Table2JoinColumn
    max_col2 = getMaxValue(openconnection, Table2JoinColumn, InputTable2)
    min_col2 = getMinValue(openconnection, Table2JoinColumn, InputTable2)

    max_col_val = max(max_col1, max_col2)
    min_col_val = min(min_col1, min_col2)

    no_threads = 5
    diff = float((max_col_val - min_col_val) / no_threads)  # range of partition

    #Create Output table using schemas of InputTable1 and InputTable2
    cur.execute("Drop table if exists " + OutputTable + ";")
    cur.execute("Create table " + OutputTable + " ( LIKE " + InputTable1 + " including all);")

    cur.execute( "Select column_name, data_type from information_schema.columns where table_name = \'" + InputTable2 + "\';")
    InputTableSchema2 = cur.fetchall()

    for i in range(len(InputTableSchema2)):
        cur.execute("Alter table " + OutputTable + " add column " + InputTableSchema2[i][0] + " " + InputTableSchema2[i][1] + ";")
      

    temp_table1= str(InputTable1)+ "_range_part_"
    temp_table2 = str(InputTable2)+ "_range_part_"
    temp_output_table= "parallel_join_temp_output_"

    #create thread list
    threadList = [0,0,0,0,0]
    for i in range(no_threads):
        min_range = min_col_val + i * diff
        max_range = min_range + diff
        threadList[i] = threading.Thread(target=createJoin, args=(InputTable1, InputTable2, OutputTable, temp_table1, temp_table2, temp_output_table, Table1JoinColumn, 
                                          Table2JoinColumn, min_range, max_range, i, openconnection))
        threadList[i].start()


    for i in range(no_threads):
        threadList[i].join()

    #Insert all temp join output tables into Output table
    for i in range(no_threads):
        cur.execute("Insert into " + OutputTable +" Select * from " + temp_output_table + str(i) + ";")

    o.commit()
    

#Function to join two single partition based on Table1JoinColumn and Table2JoinColumn using a thread
def createJoin(InputTable1, InputTable2, OutputTable, temp_table1_prefix, temp_table2_prefix, temp_output_table_prefix, Table1JoinColumn, Table2JoinColumn, min_range, 
              max_range, i, openconnection):
        
    o = openconnection	
    cur = o.cursor()

    temp_table1 = temp_table1_prefix + str(i)
    temp_table2 = temp_table2_prefix + str(i)
    temp_output_table = temp_output_table_prefix + str(i)

    #Create partition tables
    createPartitionTable(openconnection, temp_table1, InputTable1)
    createPartitionTable(openconnection, temp_table2, InputTable2)
    createPartitionTable(openconnection, temp_output_table, OutputTable)

    #Insert values to partition tables
    if i==0:
        cur.execute("Insert into " + temp_table1 + " Select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(min_range) + 
                   " and " + Table1JoinColumn + " <= " + str(max_range) + ";")
      
        cur.execute("Insert into  " + temp_table2 + " Select * from " + InputTable2 + " where " + Table2JoinColumn + " >= "+ str(min_range) + 
                  " and " + Table2JoinColumn + " <= " + str(max_range) + ";")
        
    else:
        cur.execute("Insert into " + temp_table1 + " Select * from " + InputTable1 + " where " + Table1JoinColumn + " > "+ str(min_range) + 
                    " and " + Table1JoinColumn + " <= " + str(max_range) + ";")
        
        cur.execute("Insert into " + temp_table2 +" Select * from " + InputTable2 + " where " + Table2JoinColumn + " > "+ str(min_range) + 
                   " and " + Table2JoinColumn + " <= " + str(max_range) + ";")
       

    cur.execute("Insert into  " + temp_output_table + " Select * from " + temp_table1 + " INNER JOIN " + temp_table2 + 
                   " ON " + temp_table1 + "." + Table1JoinColumn + " = " + temp_table2 + "." + Table2JoinColumn + ";")
    
    o.commit()


#Function to create new partition table
def createPartitionTable(openconnection, newTable, refTable):
    o = openconnection	
    cur = o.cursor()
    cur.execute("Drop table if exists " + newTable + ";")
    cur.execute("Create table " + newTable+ " ( LIKE " + refTable + " including all);")
    o.commit()


#Function to return minimum value of table column
def getMinValue(openconnection, col, tab):
    o = openconnection	
    cur = o.cursor()
    cur.execute("Select MIN(" + col + ") from " + tab + ";")
    return cur.fetchone()[0]

#Function to return maximum value of table column
def getMaxValue(openconnection, col, tab):
    o = openconnection	
    cur = o.cursor()
    cur.execute("Select MAX(" + col + ") from " + tab + ";")
    return cur.fetchone()[0]

###################################### DO NOT CHANGE ANYTHING BELOW THIS #####################################################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

