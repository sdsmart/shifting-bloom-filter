#!/usr/bin/env python3

# Imports
import psycopg2
import random
import time
import math
from decimal import *

# Constants
MAX_ELEMENT = 9999999999
CMS_UNIT_SIZE_IN_BITS = 32


# Main function
def main():
   
    # Get the database connection
    connection = get_db_connection()

    # Performing experiments
    getcontext().prec = 4
    shbf_m_accuracy_results = exp_shbf_m_accuracy(connection)
    print_results(shbf_m_accuracy_results)
    shbf_m_time_results = exp_shbf_m_time(connection)
    print_results(shbf_m_time_results)
    shbf_a_accuracy_results = exp_shbf_a_accuracy(connection)
    print_results(shbf_a_accuracy_results)
    shbf_a_time_results = exp_shbf_a_time(connection)
    print_results(shbf_a_time_results)
    shbf_x_accuracy_results = exp_shbf_x_accuracy(connection)
    print_results(shbf_x_accuracy_results)
    shbf_x_time_results = exp_shbf_x_time(connection)
    print_results(shbf_x_time_results)

    # Close the database connection
    connection.close()


# -------------------
# --- Experiments ---
# -------------------


# Accuracy experiment for ShBF_M and BF
def exp_shbf_m_accuracy(connection):
    
    print('running shbf_m accuracy experiment:')

	# Initialize variables
    x_values = [200*i for i in range(10, 26)]
    shbf_m_y_values = []
    bf_y_values = []

    m = 30000

	# Test the ShBF_M and BF for each x value
    for i, n in enumerate(x_values):

        o = 20000
		
		# Generate the random elements to insert and query
        total_elements = generate_elements(n + o)
        n_elements = total_elements[:n]
        o_elements = total_elements[n:]
    
		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_m_table (shbf_m_column shbf)')
        cursor.execute('INSERT INTO shbf_m_table VALUES (new_shbf_m({0}, {1}))'.format(m, n))
        cursor.execute('CREATE TABLE bf_table (bf_column bf)')
        cursor.execute('INSERT INTO bf_table VALUES (new_bf({0}, {1}))'.format(m, n))
        connection.commit()
    
		# Insert the random data into the ShBF_M
        for i, e in enumerate(n_elements):
    
            query = "UPDATE shbf_m_table SET shbf_m_column = insert_shbf_m(shbf_m_column, '{0}')".format(e) 
            cursor.execute(query)
    
		# Insert the random elements into the BF
        for i, e in enumerate(n_elements):
    
            query = "UPDATE bf_table SET bf_column = insert_bf(bf_column, '{0}')".format(e) 
            cursor.execute(query)
    
        connection.commit()
    
		# Query the ShBF_M and get the flase positives
        result = 0
        false_positives_shbf_m = 0
        for i, e in enumerate(o_elements):
    
            query = "SELECT query_shbf_m(shbf_m_column, '{0}') from shbf_m_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == 1:
                false_positives_shbf_m += 1
    
		# Query the BF and get the flase positives
        false_positives_bf = 0
        for i, e in enumerate(o_elements):
    
            query = "SELECT query_bf(bf_column, '{0}') from bf_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == 1:
                false_positives_bf += 1
    
		# Calculate the false positive percentages for both structures
        shbf_m_false_positive_percentage = (Decimal(false_positives_shbf_m) / Decimal(o)) * Decimal(100) 
        bf_false_positive_percentage = (Decimal(false_positives_bf) / Decimal(o)) * Decimal(100)
 
        shbf_m_y_values.append(str(shbf_m_false_positive_percentage))
        bf_y_values.append(str(bf_false_positive_percentage))

		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_m_table')
        cursor.execute('DROP TABLE bf_table')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()
    
	# Return the x and y values for both structures
    return [x_values, shbf_m_y_values, bf_y_values]


# Time experiment for ShBF_M and BF
def exp_shbf_m_time(connection):

    print('running shbf_m time experiment:')

	# Initialize variables
    x_values = [200*i for i in range(10, 26)]
    shbf_m_y_values = []
    bf_y_values = []

	# Test the ShBF_M and BF for each x value
    for i, n in enumerate(x_values):

        m = n * 15
        o = n * 3
    
		# Generate the random elements to insert and query
        total_elements = generate_elements(n + o)
        n_elements = total_elements[:n]
        o_elements = total_elements[n:]
    
		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_m_table (shbf_m_column shbf)')
        cursor.execute('INSERT INTO shbf_m_table VALUES (new_shbf_m({0}, {1}))'.format(m, n))
        cursor.execute('CREATE TABLE bf_table (bf_column bf)')
        cursor.execute('INSERT INTO bf_table VALUES (new_bf({0}, {1}))'.format(m, n))
        connection.commit()
    
		# Insert the random data into the ShBF_M
        for i, e in enumerate(n_elements):
    
            query = "UPDATE shbf_m_table SET shbf_m_column = insert_shbf_m(shbf_m_column, '{0}')".format(e) 
            cursor.execute(query)
    	
		# Insert the random data into the BF
        for i, e in enumerate(n_elements):
    
            query = "UPDATE bf_table SET bf_column = insert_bf(bf_column, '{0}')".format(e) 
            cursor.execute(query)
    
        connection.commit()
		
		# Query the ShBF_M and record the time elapsed
        start = time.time()
        for i, e in enumerate(total_elements):
    
            query = "SELECT query_shbf_m(shbf_m_column, '{0}') from shbf_m_table".format(e)
            cursor.execute(query)
    
        # Calculate the time elapsed
		end = time.time()
        shbf_m_time = (Decimal(end - start) / Decimal(n + o)) * Decimal(1000)
    
		# Query the BF and record the time elapsed
        start = time.time()
        for i, e in enumerate(total_elements):
    
            query = "SELECT query_bf(bf_column, '{0}') from bf_table".format(e)
            cursor.execute(query)
    
        # Calculate the time elapsed
		end = time.time()
        bf_time = (Decimal(end - start) / Decimal(n + o)) * Decimal(1000)
    
        shbf_m_y_values.append(str(shbf_m_time))
        bf_y_values.append(str(bf_time))

		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_m_table')
        cursor.execute('DROP TABLE bf_table')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()

	# Return the x and y values for both structures
    return [x_values, shbf_m_y_values, bf_y_values]


# Accuracy experiment for ShBF_A and iBF
def exp_shbf_a_accuracy(connection):

    print('running shbf_a accuracy experiment:')

	# Initialize variables
    x_values = [200*i for i in range(10, 26)]
    shbf_a_y_values = []
    ibf_y_values = []

    m = 30000
    m_ibf = 20000
	
	# Test the ShBF_A and iBF for each x value
    for i, n in enumerate(x_values):

		# Generate the random elements to insert and query
        num_s1_only_elements = int(n / 3)
        num_s2_only_elements = int(n / 3)
        num_both_elements = n - num_s1_only_elements - num_s2_only_elements
    
        num_s1_elements = num_s1_only_elements + num_both_elements
        num_s2_elements = num_s2_only_elements + num_both_elements
    
        s1_only_elements = generate_elements(num_s1_only_elements)
        s2_only_elements = generate_elements(num_s2_only_elements)
        both_elements = generate_elements(num_both_elements)
    
        s1_elements = s1_only_elements + both_elements
        s2_elements = s2_only_elements + both_elements
    
		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_a_table (shbf_a_column shbf)')
        cursor.execute('INSERT INTO shbf_a_table VALUES (new_shbf_a({0}, {1}))'.format(m, n))
        cursor.execute('CREATE TABLE bf_table_1 (bf_column bf)')
        cursor.execute('INSERT INTO bf_table_1 VALUES (new_bf({0}, {1}))'.format(m_ibf, num_s1_elements)) 
        cursor.execute('CREATE TABLE bf_table_2 (bf_column bf)')
        cursor.execute('INSERT INTO bf_table_2 VALUES (new_bf({0}, {1}))'.format(m_ibf, num_s2_elements))
        connection.commit()
    
		# Insert the random data into the ShBF_A
        for i, e in enumerate(s1_only_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 0)".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(s2_only_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 0, 1)".format(e)
            cursor.execute(query)
		
        for i, e in enumerate(both_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 1)".format(e)
            cursor.execute(query)
		
		# Insert the random elements into the BF
        for i, e in enumerate(s1_elements):
            
            query = "UPDATE bf_table_1 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(s2_elements):
            
            query = "UPDATE bf_table_2 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
            cursor.execute(query)
    
        connection.commit()
    
		# Query the ShBF_A and get the clear answers
        result = 0
        clear_shbf_a = 0
        unclear_shbf_a = 0
        for i, e in enumerate(s1_only_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == 1:
                clear_shbf_a += 1
            else:
                unclear_shbf_a += 1
    
        for i, e in enumerate(s2_only_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == 0:
                clear_shbf_a += 1
            else:
                unclear_shbf_a += 1
    
        for i, e in enumerate(both_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == 2:
                clear_shbf_a += 1
            else:
                unclear_shbf_a += 1
    
		# Query the ShBF_A and get the clear answers
        result_1 = 0
        result_2 = 0
        clear_ibf = 0
        unclear_ibf = 0
        for i, e in enumerate(s1_only_elements):
            
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
		
		# Calculate the percentage of clear answers for both structures
        shbf_a_clear_answer_percentage = Decimal(clear_shbf_a / n) * Decimal(100)
        ibf_clear_answer_percentage = Decimal(clear_ibf / n) * Decimal(100)

        shbf_a_y_values.append(str(shbf_a_clear_answer_percentage))
        ibf_y_values.append(str(ibf_clear_answer_percentage))

		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_a_table')
        cursor.execute('DROP TABLE bf_table_1')
        cursor.execute('DROP TABLE bf_table_2')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()

	# Return the x and y values for both structures
    return [x_values, shbf_a_y_values, ibf_y_values]


# Time experiment for ShBF_A and iBF
def exp_shbf_a_time(connection):

    print('running shbf_a time experiment:')

	# Initialize variables
    x_values = [200*i for i in range(10, 26)]
    shbf_a_y_values = []
    ibf_y_values = []

	# Test the ShBF_A and iBF for each x value
    for i, n in enumerate(x_values):

        m = 15 * n
    
		# Generate the random elements to insert and query
        num_s1_only_elements = int(n / 3)
        num_s2_only_elements = int(n / 3)
        num_both_elements = n - num_s1_only_elements - num_s2_only_elements
    
        num_s1_elements = num_s1_only_elements + num_both_elements
        num_s2_elements = num_s2_only_elements + num_both_elements
    
        m_ibf = 15 * num_s1_elements

        s1_only_elements = generate_elements(num_s1_only_elements)
        s2_only_elements = generate_elements(num_s2_only_elements)
        both_elements = generate_elements(num_both_elements)
    
        s1_elements = s1_only_elements + both_elements
        s2_elements = s2_only_elements + both_elements
    
		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_a_table (shbf_a_column shbf)')
        cursor.execute('INSERT INTO shbf_a_table VALUES (new_shbf_a({0}, {1}))'.format(m, n))
        cursor.execute('CREATE TABLE bf_table_1 (bf_column bf)')
        cursor.execute('INSERT INTO bf_table_1 VALUES (new_bf({0}, {1}))'.format(m_ibf, num_s1_elements)) 
        cursor.execute('CREATE TABLE bf_table_2 (bf_column bf)')
        cursor.execute('INSERT INTO bf_table_2 VALUES (new_bf({0}, {1}))'.format(m_ibf, num_s2_elements))
        connection.commit()
		
		# Insert the random data into the ShBF_A
        for i, e in enumerate(s1_only_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 0)".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(s2_only_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 0, 1)".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(both_elements):
            
            query = "UPDATE shbf_a_table SET shbf_a_column = insert_shbf_a(shbf_a_column, '{0}', 1, 1)".format(e)
            cursor.execute(query)
    
		# Insert the random data into the ShBF_A
        for i, e in enumerate(s1_elements):
            
            query = "UPDATE bf_table_1 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(s2_elements):
            
            query = "UPDATE bf_table_2 SET bf_column = insert_bf(bf_column, '{0}')".format(e)
            cursor.execute(query)
    
        connection.commit()
    
		# Query the ShBF_A and record the time elapsed
        start = time.time()
        for i, e in enumerate(s1_only_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(s2_only_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
    
        for i, e in enumerate(both_elements):
            
            query = "SELECT query_shbf_a(shbf_a_column, '{0}') from shbf_a_table".format(e)
            cursor.execute(query)
    
		# Calculate the elapsed time
        end = time.time()
        shbf_a_time = (Decimal(end - start) / Decimal(n)) * Decimal(1000)
    
		# Query the iBF and record the time elapsed
        start = time.time()
        for i, e in enumerate(s1_only_elements):
            
            query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
            query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
            cursor.execute(query1)
            cursor.execute(query2)
    
        for i, e in enumerate(s2_only_elements):
            
            query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
            query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
            cursor.execute(query1)
            cursor.execute(query2)
        
        for i, e in enumerate(both_elements):
    
            query1 = "SELECT query_bf(bf_column, '{0}') from bf_table_1".format(e) 
            query2 = "SELECT query_bf(bf_column, '{0}') from bf_table_2".format(e)
            cursor.execute(query1)
            cursor.execute(query2) 
    
		# Calculate the elapsed time
        end = time.time()
        ibf_time = (Decimal(end - start) / Decimal(n)) * Decimal(1000)

        shbf_a_y_values.append(str(shbf_a_time))
        ibf_y_values.append(str(ibf_time))
	
		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_a_table')
        cursor.execute('DROP TABLE bf_table_1')
        cursor.execute('DROP TABLE bf_table_2')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()

	# Return the x and y values for both structures
    return [x_values, shbf_a_y_values, ibf_y_values]


# Accuracy experiment for ShBF_X and CMS
def exp_shbf_x_accuracy(connection):    

    print('running shbf_x accuracy experiment:')

	# Initialize variables
    x_values = [20*i for i in range(10, 26)]
    shbf_x_y_values = []
    cms_y_values = []

	# Test the ShBF_X and CMS for each x value
    for i, n in enumerate(x_values):

        m = 15 * n
        max_x = 57
		
		# Generate the random elements to insert and query
        elements = generate_elements(n)
        counts = [random.randint(1, max_x) for i in range(n)]
      
		# Confidence level and error bound for CMS
        confidence_level = 0.15
        d = math.log(1 / (1 - confidence_level))
        w = ((m * 5) / CMS_UNIT_SIZE_IN_BITS) / d
        error_bound = math.e / w

   		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_x_table (shbf_x_column shbf)')
        cursor.execute('INSERT INTO shbf_x_table VALUES (new_shbf_x({0}, {1}, {2}))'.format(m, n, max_x))
        cursor.execute('CREATE TABLE cms_table (cms_column cms)')
        cursor.execute('INSERT INTO cms_table VALUES (new_cms({0}, {1}))'.format(error_bound, confidence_level))
        connection.commit()
    
	 	# Insert the random data into the ShBF_X
        for i, e in enumerate(elements):
            
            query = "UPDATE shbf_x_table SET shbf_x_column = insert_shbf_x(shbf_x_column, '{0}', {1})".format(e, counts[i])
            cursor.execute(query)
		
 		# Insert the random data into the CMS
        for i, e in enumerate(elements):
            
            for j in range(counts[i]):
    
                query = "UPDATE cms_table SET cms_column = insert_cms(cms_column, '{0}')".format(e)
                cursor.execute(query)
    
        connection.commit()
    
        result = 0
        exact_shbf_x = 0
        bad_shbf_x = 0
        exact_cms = 0
        bad_cms = 0
		
    	# Query the ShBF_X and CMS to get the exact answers
        for i, e in enumerate(elements):
		
			# Query the ShBF_X
            query = "SELECT query_shbf_x(shbf_x_column, '{0}') from shbf_x_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == counts[i]:
                exact_shbf_x += 1
            else:
                bad_shbf_x += 1
			
			# Query the CMS
            query = "SELECT query_cms(cms_column, '{0}') from cms_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
    
            if result == counts[i]:
                exact_cms += 1
            else:
                bad_cms += 1
		
		# Calculate the exact answer percentage for both structures
        exact_percentage_shbf_x = Decimal(exact_shbf_x / n) * Decimal(100)
        exact_percentage_cms = Decimal(exact_cms / n) * Decimal(100)

        shbf_x_y_values.append(str(exact_percentage_shbf_x))
        cms_y_values.append(str(exact_percentage_cms))
	
		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_x_table')
        cursor.execute('DROP TABLE cms_table')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()
	
	# Return the x and y values for both structures
    return [x_values, shbf_x_y_values, cms_y_values]


# Time experiment for ShBF_X and CMS
def exp_shbf_x_time(connection):    

    print('running shbf_x time experiment:')
	
	# Initialize variables
    x_values = [20*i for i in range(10, 26)]
    shbf_x_y_values = []
    cms_y_values = []

	# Test the ShBF_X and CMS for each x value
    for i, n in enumerate(x_values):

        m = 15 * n
        max_x = 57
    
		# Generate the random elements to insert and query
        elements = generate_elements(n)
        counts = [random.randint(1, max_x) for i in range(n)]
      
		# Confidence level and error bound for CMS
        confidence_level = 0.10
        d = math.log(1 / (1 - confidence_level))
        w = ((m * 5) / CMS_UNIT_SIZE_IN_BITS) / d
        error_bound = math.e / w
    
		# Create the database extension and tables
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION shbf')
        cursor.execute('CREATE TABLE shbf_x_table (shbf_x_column shbf)')
        cursor.execute('INSERT INTO shbf_x_table VALUES (new_shbf_x({0}, {1}, {2}))'.format(m, n, max_x))
        cursor.execute('CREATE TABLE cms_table (cms_column cms)')
        cursor.execute('INSERT INTO cms_table VALUES (new_cms({0}, {1}))'.format(error_bound, confidence_level))
        connection.commit()
    
		# Insert the random data into the ShBF_X
        for i, e in enumerate(elements):
            
            query = "UPDATE shbf_x_table SET shbf_x_column = insert_shbf_x(shbf_x_column, '{0}', {1})".format(e, counts[i])
            cursor.execute(query)
    
		# Insert the random data into the CMS
        for i, e in enumerate(elements):
            
            for j in range(counts[i]):
    
                query = "UPDATE cms_table SET cms_column = insert_cms(cms_column, '{0}')".format(e)
                cursor.execute(query)
    
        connection.commit()
    
        result = 0
    
		# Query the ShBF_X and record the time elapsed
        start = time.time()
        for i, e in enumerate(elements):
    
            query = "SELECT query_shbf_x(shbf_x_column, '{0}') from shbf_x_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
        
		# Calculate the elapsed time
        end = time.time()
        shbf_x_time = (Decimal(end - start) / Decimal(n)) * Decimal(1000)
    
		# Query the CMS and record the time elapsed
        start = time.time()
        for i, e in enumerate(elements):
    
            query = "SELECT query_cms(cms_column, '{0}') from cms_table".format(e)
            cursor.execute(query)
            result = cursor.fetchone()[0]
        
		# Calculate the elapsed time
        end = time.time()
        cms_time = (Decimal(end - start) / Decimal(n)) * Decimal(1000)
   
        shbf_x_y_values.append(str(shbf_x_time))
        cms_y_values.append(str(cms_time))
	
		# Clean up PostgreSQL by removing the tables and extension
        cursor.execute('DROP TABLE shbf_x_table')
        cursor.execute('DROP TABLE cms_table')
        cursor.execute('DROP EXTENSION shbf')
        connection.commit()
		
	# Return the x and y values for both structures
    return [x_values, shbf_x_y_values, cms_y_values]


# ------------------------
# --- Utility function ---
# ------------------------

# Generate n unique elements
def generate_elements(n):
    
    elements = set()

    while len(elements) < n:
        elements.add(str(random.randint(1, MAX_ELEMENT)))

    return list(elements)


# Initialize database connection
def get_db_connection():

    try:
        connection = psycopg2.connect("dbname='postgres' user='postgres'")
    except:
        print('Unable to connect to database')
        quit()

    return connection


# Print experimental results
def print_results(results):

    print('x_values = {0};'.format(str(results[0])))
    print('shbf_y_values = {0};'.format((str(results[1])).replace("'", '')))
    print('other_y_values = {0};'.format((str(results[2])).replace("'", '')))

# Execute main function
if __name__ == '__main__':
    main()
