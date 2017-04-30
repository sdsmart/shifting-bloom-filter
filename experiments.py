#!/usr/bin/env python3


# Imports
import psycopg2
import random
import time


# Constants
MAX_ELEMENT = 9999999999


# Main function
def main():
   
    # Grabbing the connection to the database
    connection = get_db_connection()

    # Performing experiments
    test_bf(connection)
    test_shbf_m(connection)

    # Closing the database connection
    connection.close()


# -------------------
# --- Experiments ---
# -------------------


# TODO
def test_bf(connection):
    
    # Printing test header
    print('============ TESTING BF ============')

    # Setting up local variables
    m = 120000
    n = 10000

    num_elements_1 = n
    num_elements_2 = 100000
    num_total_elements = num_elements_1 + num_elements_2

    total_elements = generate_elements(num_total_elements)
    elements_1 = total_elements[:num_elements_1]
    elements_2 = total_elements[num_elements_1:]

    true_positives = 0
    false_negatives = num_elements_1
    false_positives = 0
    true_negatives = num_elements_2
    result = 0

    # Creating postgres extension and an empty bloom filter
    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE bf_table (bf_column bf)')
    cursor.execute('INSERT INTO bf_table VALUES (new_bf({0}, {1}))'.format(m, n))
    connection.commit()

    # --- TEST 1 ---

    start = time.time()

    # Inserting elements into bloom filter
    for i, e in enumerate(elements_1):

        if i % 1000 == 0 and i > 0:
            print('iteration: {0}'.format(i))

        query = '''UPDATE bf_table
                       SET bf_column = insert_bf(bf_column, '{0}')'''.format(e) 
        cursor.execute(query)

    connection.commit()

    end = time.time()
    print('BF insertion time: {0}'.format(end - start))
    
    # Querying the inserted elements to confirm that the bloom filter returns true
    for e in elements_1:

        query = "SELECT query_bf(bf_column, '{0}') from bf_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]
        
        #break

        true_positives += result
        false_negatives -= result

    # Printing results of TEST 1
    print('---------- TEST 1 RESULTS ----------')
    print('True positives: {0}'.format(true_positives))
    print('False negatives: {0}'.format(false_negatives))
    print('------------------------------------\n')

    # --- TEST 2 ---

    # Querying other elements that were not inserted into the bloom filter
    for i, e in enumerate(elements_2):

        #break

        if i % 10000 == 0 and i > 0:
            print('iteration: {0}'.format(i))

        query = "SELECT query_bf(bf_column, '{0}') from bf_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]

        false_positives += result
        true_negatives -= result

    # Printing results of TEST 2
    print('\n---------- TEST 2 RESULTS ----------')
    print('True negatives: {0}'.format(true_negatives))
    print('False positives: {0}'.format(false_positives))
    print('------------------------------------') 
    print('====================================')

    cursor.execute('DROP TABLE bf_table')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


# TODO
def test_shbf_m(connection):
    
    # Printing test header
    print('========== TESTING ShBF_M ==========')

    # Setting up local variables
    m = 120000
    n = 10000

    num_elements_1 = n
    num_elements_2 = 100000
    num_total_elements = num_elements_1 + num_elements_2

    total_elements = generate_elements(num_total_elements)
    elements_1 = total_elements[:num_elements_1]
    elements_2 = total_elements[num_elements_1:]

    true_positives = 0
    false_negatives = num_elements_1
    false_positives = 0
    true_negatives = num_elements_2
    result = 0

    # Creating postgres extension and an empty bloom filter
    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_m_table (shbf_m_column shbf)')
    cursor.execute('INSERT INTO shbf_m_table VALUES (new_shbf_m({0}, {1}))'.format(m, n))
    connection.commit()

    # --- TEST 1 ---

    start = time.time()

    # Inserting elements into bloom filter
    for i, e in enumerate(elements_1):

        if i % 1000 == 0 and i > 0:
            print('iteration: {0}'.format(i))

        query = '''UPDATE shbf_m_table
                       SET shbf_m_column = insert_shbf_m(shbf_m_column, '{0}')'''.format(e) 
        cursor.execute(query)

    connection.commit()

    end = time.time()
    print('ShBF_M insertion time: {0}'.format(end - start))
    
    # Querying the inserted elements to confirm that the bloom filter returns true
    for e in elements_1:

        query = "SELECT query_shbf_m(shbf_m_column, '{0}') from shbf_m_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]
        
        #break

        true_positives += result
        false_negatives -= result

    # Printing results of TEST 1
    print('---------- TEST 1 RESULTS ----------')
    print('True positives: {0}'.format(true_positives))
    print('False negatives: {0}'.format(false_negatives))
    print('------------------------------------\n')
    
    # --- TEST 2 ---

    # Querying other elements that were not inserted into the bloom filter
    for i, e in enumerate(elements_2):

        #break

        if i % 10000 == 0 and i > 0:
            print('iteration: {0}'.format(i))

        query = "SELECT query_shbf_m(shbf_m_column, '{0}') from shbf_m_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]

        false_positives += result
        true_negatives -= result

    # Printing results of TEST 2
    print('\n---------- TEST 2 RESULTS ----------')
    print('True negatives: {0}'.format(true_negatives))
    print('False positives: {0}'.format(false_positives))
    print('------------------------------------')
    print('====================================')

    cursor.execute('DROP TABLE shbf_m_table')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


# ------------------------
# --- Utility function ---
# ------------------------

# Generate n unique elements
def generate_elements(n):
    
    elements = set()

    while len(elements) < n:
        elements.add(str(random.randint(1, MAX_ELEMENT)))

    return list(elements)


# Initializing database connection
def get_db_connection():

    try:
        connection = psycopg2.connect("dbname='postgres' user='postgres'")
    except:
        print('Unable to connect to database')
        quit()

    return connection


# Executing main function
if __name__ == '__main__':
    main()
