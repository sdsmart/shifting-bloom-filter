#!/usr/bin/env python3


# Imports
import psycopg2
import random


# Constants
MAX_ELEMENT = 9999999999


# Main function
def main():
   
    # Grabbing the connection to the database
    connection = get_db_connection()

    # Performing experiments for membership
    run_membership_experiments(connection)

    # Closing the database connection
    connection.close()

# -------------------
# --- Experiments ---
# -------------------


# Function to run membership experiments
def run_membership_experiments(connection):
    
    # Setting up local variables
    num_elements_1 = 100
    num_elements_2 = 100000
    num_total_elements = num_elements_1 + num_elements_2

    total_elements = generate_elements(num_total_elements)
    elements_1 = total_elements[:100]
    elements_2 = total_elements[100:]

    m = 1000
    n = num_elements_1

    true_positives = 0
    false_negatives = num_elements_1
    false_positives = 0
    true_negatives = num_elements_2

    # Creating postgres extension and an empty bloom filter
    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE bloom_filter_table (bloom_filter bf)')
    cursor.execute('INSERT INTO bloom_filter_table VALUES (new_bf({0}, {1}))'.format(m, n))
    connection.commit()

    # --- TEST 1 ---

    # Inserting elements into bloom filter
    for e in elements_1:

        query = '''UPDATE bloom_filter_table
                       SET bloom_filter = insert_bf(bloom_filter, '{0}')'''.format(e)
        cursor.execute(query)

    connection.commit()
    
    # Querying the inserted elements to confirm that the bloom filter returns true
    for e in elements_1:

        query = "SELECT query_bf(bloom_filter, '{0}') from bloom_filter_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]

        true_positives += result
        false_negatives -= result

    # Printing results of TEST 1
    print('=============== TEST 1 RESULTS ===============')
    print('True positives: {0}'.format(true_positives))
    print('False negatives: {0}\n'.format(false_negatives))

    # --- TEST 2 ---

    # Querying other elements that were not inserted into the bloom filter
    for i, e in enumerate(elements_2):
  
        if i % 10000 == 0 and i > 0:
            print('iteration: {0}'.format(i))

        query = "SELECT query_bf(bloom_filter, '{0}') from bloom_filter_table".format(e)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]

        false_positives += result
        true_negatives -= result

    # Printing results of TEST 2
    print('/n=============== TEST 2 RESULTS ===============')
    print('True negatives: {0}'.format(true_negatives))
    print('False positives: {0}'.format(false_positives))

    cursor.execute('DROP TABLE bloom_filter_table')
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

    return connection


# Executing main function
if __name__ == '__main__':
    main()
