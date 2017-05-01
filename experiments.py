#!/usr/bin/env python3


# Imports
import psycopg2
import random
import time
import math


# Constants
MAX_ELEMENT = 9999999999
CMS_UNIT_SIZE_IN_BITS = 32

# Main function
def main():
   
    # Grabbing the connection to the database
    connection = get_db_connection()

    # Performing experiments
    #exp_shbf_m_accuracy(connection)
    exp_shbf_m_time(connection)
    #exp_shbf_a_accuracy(connection)
    #exp_shbf_a_time(connection)
    #exp_shbf_x_accuracy(connection)
    #exp_shbf_x_time(connection)

    # Closing the database connection
    connection.close()


# -------------------
# --- Experiments ---
# -------------------


# TODO
def exp_shbf_m_accuracy(connection):
    
    m = 150000
    n = 10000
    o = n * 10

    total_elements = generate_elements(n + o)
    n_elements = total_elements[:n]
    o_elements = total_elements[n:]

    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_m_table (shbf_m_column shbf)')
    cursor.execute('INSERT INTO shbf_m_table VALUES (new_shbf_m({0}, {1}))'.format(m, n))
    cursor.execute('CREATE TABLE bf_table (bf_column bf)')
    cursor.execute('INSERT INTO bf_table VALUES (new_bf({0}, {1}))'.format(m, n))
    connection.commit()

    print('inserting shbf_m elements...')

    for i, e in enumerate(n_elements):

        #if i % 1000 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "UPDATE shbf_m_table SET shbf_m_column = insert_shbf_m(shbf_m_column, '{0}')".format(e) 
        cursor.execute(query)
 
    print('inserting bf elements...')

    for i, e in enumerate(n_elements):

        #if i % 1000 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "UPDATE bf_table SET bf_column = insert_bf(bf_column, '{0}')".format(e) 
        cursor.execute(query)

    connection.commit()

    print('querying shbf_m elements...')

    result = 0
    false_positives_shbf_m = 0

    for i, e in enumerate(o_elements):

        #if i % 1000 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "SELECT query_shbf_m(shbf_m_column, '{0}') from shbf_m_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == 1:
            false_positives_shbf_m += 1

    print('querying bf elements...')

    false_positives_bf = 0

    for i, e in enumerate(o_elements):

        #if i % 1000 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "SELECT query_bf(bf_column, '{0}') from bf_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == 1:
            false_positives_bf += 1

    print('shbf_m false positive %: {0}'.format(float((false_positives_shbf_m / o) * 100)))
    print('bf false positive %:     {0}'.format(float((false_positives_bf / o) * 100)))

    cursor.execute('DROP TABLE shbf_m_table')
    cursor.execute('DROP TABLE bf_table')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


# TODO
def exp_shbf_a_accuracy(connection):

    m = 150000
    n = 9999

    num_s1_only_elements = n / 3
    num_s2_only_elements = n / 3
    num_both_elements = n / 3

    num_s1_elements = num_s1_only_elements + num_both_elements
    num_s2_elements = num_s2_only_elements + num_both_elements

    s1_only_elements = generate_elements(num_s1_only_elements)
    s2_only_elements = generate_elements(num_s2_only_elements)
    both_elements = generate_elements(num_both_elements)

    s1_elements = s1_only_elements + both_elements
    s2_elements = s2_only_elements + both_elements

    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_a_table (shbf_a_column shbf)')
    cursor.execute('INSERT INTO shbf_a_table VALUES (new_shbf_a({0}, {1}))'.format(m, n))
    cursor.execute('CREATE TABLE bf_table_1 (bf_column bf)')
    cursor.execute('INSERT INTO bf_table_1 VALUES (new_bf({0}, {1}))'.format(m, n)) 
    cursor.execute('CREATE TABLE bf_table_2 (bf_column bf)')
    cursor.execute('INSERT INTO bf_table_2 VALUES (new_bf({0}, {1}))'.format(m, n))
    connection.commit()

    print('inserting shbf_a elements...')

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 0)".format(e)
        cursor.execute(query)

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 0, 1)".format(e)
        cursor.execute(query)

    for i, e in enumerate(both_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 1)".format(e)
        cursor.execute(query)

    print('inserting ibf elements...')

    for i, e in enumerate(s1_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE bf_table_1 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
        cursor.execute(query)

    for i, e in enumerate(s2_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE bf_table_2 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
        cursor.execute(query)

    connection.commit()

    print('querying shbf_a elements...')

    result = 0
    clear_shbf_a = 0
    unclear_shbf_a = 0

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == 1:
            clear_shbf_a += 1
        else:
            unclear_shbf_a += 1

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == 0:
            clear_shbf_a += 1
        else:
            unclear_shbf_a += 1

    for i, e in enumerate(both_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == 2:
            clear_shbf_a += 1
        else:
            unclear_shbf_a += 1

    print('querying ibf elements...')

    result_1 = 0
    result_2 = 0
    clear_ibf = 0
    unclear_ibf = 0

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
        query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)

        cursor.execute(query1)
        result_1 = cursor.fetchone()[0]

        cursor.execute(query2)
        result_2 = cursor.fetchone()[0]

        if result_1 == 1 and result_2 == 0:
            clear_ibf += 1
        else:
            unclear_ibf += 1

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
        query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)

        cursor.execute(query1)
        result_1 = cursor.fetchone()[0]

        cursor.execute(query2)
        result_2 = cursor.fetchone()[0]

        if result_1 == 0 and result_2 == 1:
            clear_ibf += 1
        else:
            unclear_ibf += 1

    unclear_ibf += num_both_elements

    print('shbf_a clear answer %:   {0}'.format(float((clear_shbf_a / n) * 100)))
    print('ibf clear answer %:      {0}'.format(float((clear_ibf / n) * 100)))

    cursor.execute('DROP TABLE shbf_a_table')
    cursor.execute('DROP TABLE bf_table_1')
    cursor.execute('DROP TABLE bf_table_2')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


# TODO
def exp_shbf_a_time(connection):

    m = 1500000
    n = 99999

    num_s1_only_elements = n / 3
    num_s2_only_elements = n / 3
    num_both_elements = n / 3

    num_s1_elements = num_s1_only_elements + num_both_elements
    num_s2_elements = num_s2_only_elements + num_both_elements

    s1_only_elements = generate_elements(num_s1_only_elements)
    s2_only_elements = generate_elements(num_s2_only_elements)
    both_elements = generate_elements(num_both_elements)

    s1_elements = s1_only_elements + both_elements
    s2_elements = s2_only_elements + both_elements

    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_a_table (shbf_a_column shbf)')
    cursor.execute('INSERT INTO shbf_a_table VALUES (new_shbf_a({0}, {1}))'.format(m, n))
    cursor.execute('CREATE TABLE bf_table_1 (bf_column bf)')
    cursor.execute('INSERT INTO bf_table_1 VALUES (new_bf({0}, {1}))'.format(m, n)) 
    cursor.execute('CREATE TABLE bf_table_2 (bf_column bf)')
    cursor.execute('INSERT INTO bf_table_2 VALUES (new_bf({0}, {1}))'.format(m, n))
    connection.commit()

    print('inserting shbf_a elements...')

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 0)".format(e)
        cursor.execute(query)

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 0, 1)".format(e)
        cursor.execute(query)

    for i, e in enumerate(both_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 1)".format(e)
        cursor.execute(query)

    print('inserting ibf elements...')

    for i, e in enumerate(s1_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE bf_table_1 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
        cursor.execute(query)

    for i, e in enumerate(s2_elements):
        
        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE bf_table_2 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
        cursor.execute(query)

    connection.commit()

    print('querying shbf_a elements...')

    start = time.time()

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)

    for i, e in enumerate(both_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
        cursor.execute(query)

    end = time.time()

    shbf_a_time = end - start

    print('querying ibf elements...')

    start = time.time()

    for i, e in enumerate(s1_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
        query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
        cursor.execute(query1)
        cursor.execute(query2)

    for i, e in enumerate(s2_only_elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
        query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
        cursor.execute(query1)
        cursor.execute(query2)
    
    for i, e in enumerate(both_elements):
 
        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
        query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
        cursor.execute(query1)
        cursor.execute(query2) 

    end = time.time()

    ibf_time = end - start

    print('shbf_a time: {0}'.format(shbf_a_time))
    print('ibf time: {0}'.format(ibf_time))

    cursor.execute('DROP TABLE shbf_a_table')
    cursor.execute('DROP TABLE bf_table_1')
    cursor.execute('DROP TABLE bf_table_2')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


# TODO
def exp_shbf_x_accuracy(connection):    

    m = 15000
    n = 1000
    max_x = 57

    elements = generate_elements(n)
    counts = [random.randint(1, max_x) for i in range(n)]
  
    confidence_level = 0.05
    d = math.log(1 / (1 - confidence_level))
    w = (m / CMS_UNIT_SIZE_IN_BITS) / d
    error_bound = math.e / w

    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_x_table (shbf_x_column shbf)')
    cursor.execute('INSERT INTO shbf_x_table VALUES (new_shbf_x({0}, {1}, {2}))'.format(m, n, max_x))
    cursor.execute('CREATE TABLE cms_table (cms_column cms)')
    cursor.execute('INSERT INTO cms_table VALUES (new_cms({0}, {1}))'.format(error_bound, confidence_level))
    connection.commit()

    print('inserting shbf_x elements...')

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_x_table SET shbf_x_column = insert_shbf_x(shbf_x_column, '{0}', {1})".format(e, counts[i])
        cursor.execute(query)

    print('inerting cms elements...')

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        for j in range(counts[i]):

            query = "UPDATE cms_table SET cms_column = insert_cms(cms_column, '{0}')".format(e)
            cursor.execute(query)

    connection.commit()

    result = 0
    exact_shbf_x = 0
    bad_shbf_x = 0
    exact_cms = 0
    bad_cms = 0

    print('querying elements...')

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "SELECT query_shbf_x(shbf_x_column, '{0}') from shbf_x_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == counts[i]:
            exact_shbf_x += 1
        else:
            bad_shbf_x += 1

        query = "SELECT query_cms(cms_column, '{0}') from cms_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result == counts[i]:
            exact_cms += 1
        else:
            bad_cms += 1

    exact_percentage_shbf_x = float((exact_shbf_x / n) * 100)
    exact_percentage_cms = float((exact_cms / n) * 100)

    print('Exact % ShBF_X:  {0}'.format(exact_percentage_shbf_x))
    print('Exact % CMS:     {0}'.format(exact_percentage_cms))
    
    cursor.execute('DROP TABLE shbf_x_table')
    cursor.execute('DROP TABLE cms_table')
    cursor.execute('DROP EXTENSION shbf')
    connection.commit()


#TODO
def exp_shbf_x_time(connection):    

    m = 15000
    n = 1000
    max_x = 57

    elements = generate_elements(n)
    counts = [random.randint(1, max_x) for i in range(n)]
  
    confidence_level = 0.05
    d = math.log(1 / (1 - confidence_level))
    w = (m / CMS_UNIT_SIZE_IN_BITS) / d
    error_bound = math.e / w

    cursor = connection.cursor()
    cursor.execute('CREATE EXTENSION shbf')
    cursor.execute('CREATE TABLE shbf_x_table (shbf_x_column shbf)')
    cursor.execute('INSERT INTO shbf_x_table VALUES (new_shbf_x({0}, {1}, {2}))'.format(m, n, max_x))
    cursor.execute('CREATE TABLE cms_table (cms_column cms)')
    cursor.execute('INSERT INTO cms_table VALUES (new_cms({0}, {1}))'.format(error_bound, confidence_level))
    connection.commit()

    print('inserting shbf_x elements...')

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        query = "UPDATE shbf_x_table SET shbf_x_column = insert_shbf_x(shbf_x_column, '{0}', {1})".format(e, counts[i])
        cursor.execute(query)

    print('inserting cms elements...')

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))
        
        for j in range(counts[i]):

            query = "UPDATE cms_table SET cms_column = insert_cms(cms_column, '{0}')".format(e)
            cursor.execute(query)

    connection.commit()

    result = 0

    print('querying shbf_x elements...')

    start = time.time()
    
    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "SELECT query_shbf_x(shbf_x_column, '{0}') from shbf_x_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]
    
    end = time.time()

    shbf_x_time = end - start

    print('querying cms elements...')

    start = time.time()

    for i, e in enumerate(elements):

        #if i % 100 == 0 and i > 0:
        #    print('iteration: {0}'.format(i))

        query = "SELECT query_cms(cms_column, '{0}') from cms_table".format(e)
        cursor.execute(query)
        result = cursor.fetchone()[0]
    
    end = time.time()

    cms_time = end - start

    print('shbf_x time: {0}'.format(shbf_x_time))
    print('cms time: {0}'.format(cms_time))

    cursor.execute('DROP TABLE shbf_x_table')
    cursor.execute('DROP TABLE cms_table')
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
