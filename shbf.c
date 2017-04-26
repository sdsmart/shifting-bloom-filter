#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "MurmurHash3.h"

#define MURMUR_HASH_SEED 304837963

#define K_OPT_M 0.7009
#define K_OPT_A 0.6931
#define K_OPT_X 0.6931
#define W 57

#define ELEMENT_LENGTH 10

/* General shifting bloom filter struct */
typedef struct ShBF {
    unsigned char* B;
    int B_length;
    int m;
    int n;
    int k;
    int w;
} ShBF;


/* Function prototypes */
ShBF* new_ShBF_M(int m, int n);
void insert_ShBF_M(ShBF* shbf_m, char* e);
int query_ShBF_M(ShBF* shbf_m, char* e);

ShBF* new_ShBF_A(int m, int n);
void insert_ShBF_A(ShBF* shbf_a, char* e, int s1, int s2);
int query_ShBF_A(ShBF* shbf_a, char* e);

ShBF* new_ShBF_X(int m, int n, int max_x);
void insert_ShBF_X(ShBF* shbf_x, char* e, int x);
int query_ShBF_X(ShBF* shbf_x, char* e);

int get_bit_ShBF(ShBF* shbf, int index);
void set_bit_ShBF(ShBF* shbf, int index);
void print_ShBF(ShBF* shbf);
void free_ShBF(ShBF* shbf);

void test_ShBF_M();
void test_ShBF_A();
void test_ShBF_X();
char** generate_elements(int n);
char* generate_element();


/* Main  - Executes tests */
int main() {

    srand(time(NULL));

    test_ShBF_X();
   
    return 0;
}


/* Constructor for ShBF_M */
ShBF* new_ShBF_M(int m, int n) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    unsigned char* B;
    int B_size;

    k_unrounded = (K_OPT_M * ((float)m / n)) / 2;
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = m + (W - 1);
    B_size = ((B_size % 8) == 0) ? B_size : ((B_size / 8) + 1) * 8;
    B = (unsigned char*)malloc(B_size);

    shbf = (ShBF*)malloc(B_size + (5 * sizeof(int)));
    shbf->B = B;
    shbf->B_length = (B_size / 8);
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = W;

    return shbf;
}


/* Inserts an element into ShBF_M */
void insert_ShBF_M(ShBF* shbf_m, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t offset_hash;
    uint64_t i_hash;
    int offset_value;
    int first_index;
    int second_index;
    int i;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    offset_hash = hva[0];
    offset_value = (offset_hash % (shbf_m->w - 1)) + 1;

    for (i = 1; i <= shbf_m->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        first_index = i_hash % shbf_m->m;
        second_index = first_index + offset_value;
    
        set_bit_ShBF(shbf_m, first_index);
        set_bit_ShBF(shbf_m, second_index);
    }
}


/* Queries ShBF_M to determine if a given element is present */
int query_ShBF_M(ShBF* shbf_m, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t offset_hash;
    uint64_t i_hash;
    int offset_value;
    int first_index;
    int second_index;
    int i;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    offset_hash = hva[0];
    offset_value = (offset_hash % (shbf_m->w - 1)) + 1;

    for (i = 1; i <= shbf_m->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        first_index = i_hash % shbf_m->m;
        second_index = first_index + offset_value;
    
        if (get_bit_ShBF(shbf_m, first_index) != 1 || 
            get_bit_ShBF(shbf_m, second_index) != 1) { 
        
            return 0;
        }
    }

    return 1;
}


/* Constructor for ShBF_A */
ShBF* new_ShBF_A(int m, int n) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    unsigned char* B;
    int B_size;

    k_unrounded = K_OPT_A * ((float)m / n);
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = m + (W - 2);
    B_size = ((B_size % 8) == 0) ? B_size : ((B_size / 8) + 1) * 8;
    B = (unsigned char*)malloc(B_size);

    shbf = (ShBF*)malloc(B_size + (5 * sizeof(int)));
    shbf->B = B;
    shbf->B_length = (B_size / 8);
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = W;

    return shbf;
}


/* Inserts an element into ShBF_A */
void insert_ShBF_A(ShBF* shbf_a, char* e, int s1, int s2) { 

    uint64_t hva[2] = {0, 0};
    uint64_t first_offset_hash;
    uint64_t second_offset_hash;
    uint64_t i_hash;
    int first_offset_value;
    int second_offset_value;
    int offset_value;
    int index;
    int i;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    first_offset_hash = hva[0];
    second_offset_hash = hva[1];

    first_offset_value = (first_offset_hash % ((shbf_a->w - 1) / 2)) + 1;
    second_offset_value = first_offset_value 
                        + (second_offset_hash % ((shbf_a->w - 1) / 2)) + 1;

    if (s1 && s2) { offset_value = first_offset_value;  } 
    else if (s1)  { offset_value = 0;                   } 
    else if (s2)  { offset_value = second_offset_value; }
    
    for (i = 1; i <= shbf_a->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        index = i_hash % shbf_a->m + offset_value;
    
        set_bit_ShBF(shbf_a, index);
    }
}


/* Queries ShBF_A to determine the association info for a given element */
int query_ShBF_A(ShBF* shbf_a, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t first_offset_hash;
    uint64_t second_offset_hash;
    uint64_t i_hash;
    int first_offset_value;
    int second_offset_value;
    int first_index;
    int second_index;
    int third_index;
    int both;
    int s1_only;
    int s2_only;
    int i;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    first_offset_hash = hva[0];
    second_offset_hash = hva[1];

    first_offset_value = (first_offset_hash % ((shbf_a->w - 1) / 2)) + 1;
    second_offset_value = first_offset_value 
                        + (second_offset_hash % ((shbf_a->w - 1) / 2)) + 1;

    both = 1;
    s1_only = 1;
    s2_only = 1;
    for (i = 1; i <= shbf_a->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        first_index = i_hash % shbf_a->m;
        second_index = first_index + first_offset_value;
        third_index = first_index + second_offset_value;
    
        if (get_bit_ShBF(shbf_a, first_index)  != 1) { s1_only = 0; }
        if (get_bit_ShBF(shbf_a, second_index) != 1) { both = 0;    }
        if (get_bit_ShBF(shbf_a, third_index)  != 1) { s2_only = 0; }
    }

    if (both && s1_only && s2_only) { return 6; }
    else if (both && s1_only)       { return 5; }
    else if (both && s2_only)       { return 4; }
    else if (s1_only && s2_only)    { return 3; }
    else if (both)                  { return 2; }
    else if (s1_only)               { return 1; }
    else if (s2_only)               { return 0; }
}


/* Constructor for ShBF_X */
ShBF* new_ShBF_X(int m, int n, int max_x) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    unsigned char* B;
    int B_size;
    
    k_unrounded = K_OPT_X * ((float)m / n);
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = (m + (max_x - 1));
    B_size = ((B_size % 8) == 0) ? B_size : ((B_size / 8) + 1) * 8;
    B = (unsigned char*)malloc(B_size);

    shbf = (ShBF*)malloc(B_size + (5 * sizeof(int)));
    shbf->B = B;
    shbf->B_length = (B_size / 8);
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = max_x;

    return shbf;
}


/* Inserts an element into ShBF_X */
void insert_ShBF_X(ShBF* shbf_x, char* e, int x) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    int offset_value;
    int index;
    int i;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    offset_value = x - 1;

    for (i = 0; i < shbf_x->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];
        index = i_hash % shbf_x->m + offset_value;

        set_bit_ShBF(shbf_x, index);
    }    
}


/* Queries ShBF_X to determine the multiplicity for a given element */
int query_ShBF_X(ShBF* shbf_x, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    uint64_t hash_indexes[shbf_x->k];
    int multiplicity = 0;   
    int ones_flag;    
    int i;
    int j;

    MurmurHash3_x64_128(e, (sizeof(e) / sizeof(char)), MURMUR_HASH_SEED, &hva);

    for (i = 0; i < shbf_x->k; i++) {

        i_hash = hva[0] + i*hva[1];
        hash_indexes[i] = i_hash % shbf_x->m;
    }

    for (i = 0; i < shbf_x->w; i++) { 
    
        ones_flag = 1;

        for (j = 0; j < shbf_x->k; j++) {

            if (get_bit_ShBF(shbf_x, (hash_indexes[j] + i)) != 1) { 
        
                ones_flag = 0;
                break;
            }
        }

        if (ones_flag == 1) { multiplicity = i + 1; }
    }

    return multiplicity;
}


/* Gets the bit at the specified index in the ShBF array */
int get_bit_ShBF(ShBF* shbf, int index) {

    int B_index;
    int byte_offset;
    unsigned char byte;    
    int bit;

    B_index = index / 8;
    byte_offset = index % 8;

    byte = shbf->B[B_index];
    bit = (byte >> (7 - byte_offset)) & 1;

    return bit;
}


/* Sets the bit at the specified index in the ShBF array to 1 */
void set_bit_ShBF(ShBF* shbf, int index) {

    int B_index;
    int byte_offset;
    unsigned char mask = 128;    
    int bit;

    B_index = index / 8;
    byte_offset = index % 8;

    mask >>= byte_offset;
    shbf->B[B_index] = shbf->B[B_index] | mask;
}


/* Prints the contents of a general shifting bloom filter */
void print_ShBF(ShBF* shbf) {

    int ones;
    int zeros;
    int bit;
    int i;    

    ones = 0;
    zeros = 0;
    for (i = 0; i < shbf->B_length*8; i++) { 
        
        bit = get_bit_ShBF(shbf, i);

        if (bit == 0) { zeros++; }
        else if (bit == 1) { ones++; }
        else { printf("ERROR - BIT NOT 0 or 1"); }
    }

    printf("====== ShBF Contents ======\n");
    printf("Number of 1's: %d\n", ones);
    printf("Number of 0's: %d\n", zeros);
    printf("B_length: %d\nm: %d\nn: %d\nk: %d\n", shbf->B_length, shbf->m, 
                                                  shbf->n, shbf->k);
    printf("===========================\n\n");
}


/* Destructor for general shifting bloom filter struct */
void free_ShBF(ShBF* shbf) {

    free(shbf->B);
    free(shbf);
}


/* Test ShBF_M */
void test_ShBF_M() {

    int i;
    int num_elements_1 = 100;
    int num_elements_2 = 1000000;
    int num_elements = num_elements_1 + num_elements_2;
    char** elements;
    char** elements_1;
    char** elements_2;
    ShBF* test;
    int query_result;
    int in_set = 0;
    int out_of_set = num_elements_1;
    int tp = 0;
    int fp = 0;
    int tn = 0;
    int fn = 0;
    int positive;
    int j;

    elements = generate_elements(num_elements);
    elements_1 = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * num_elements_1);
    elements_2 = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * num_elements_2);

    for (i = 0; i < num_elements_1; i++) { elements_1[i] = elements[i]; }
    for (i = 0; i < num_elements_2; i++) { elements_2[i] = elements[i + num_elements_1]; }

    test = new_ShBF_M(1285, num_elements_1);

    printf("===== BEFORE INSERTION ====\n");
    print_ShBF(test);

    for (i = 0; i < num_elements_1; i++) {
    
        insert_ShBF_M(test, elements_1[i]);        
    }

    printf("===== AFTER INSERTION =====\n");
    print_ShBF(test);

    for (i = 0; i < num_elements_1; i++) {
    
        query_result = query_ShBF_M(test, elements_1[i]);
        in_set += query_result;
        out_of_set -= query_result;
    }

    printf("===== TEST 1 RESULTS - QUERYING INSERTED ELEMENTS =====\n");
    printf("Number of elements in set     : %d\n", in_set);
    printf("Number of elements not in set : %d\n\n", out_of_set);

    for (i = 0; i < num_elements_2; i++) {

        if (i % 100 == 0) { printf("i : %d\n", i); }

        query_result = query_ShBF_M(test, elements_2[i]);

        positive = 0;
        for (j = 0; j < num_elements_1; j++) {

            if (!strcmp(elements_2[i], elements_1[j])) { positive = 1; break;}
        }

        if (positive && query_result) { tp++; }
        else if (positive) { fn++; }
        else if (query_result) { fp++; }
        else { tn++; }
    }

    printf("===== TEST 2 RESULTS - QUERYING %d RANDOM ELEMENTS =====\n", num_elements_2);
    printf("True Positives: %d\n", tp);
    printf("False Positives: %d\n", fp);
    printf("True Negatives: %d\n", tn);
    printf("False Negatives: %d\n", fn);  

    free_ShBF(test);
    for (i = 0; i < num_elements; i++) { free(elements[i]); }
    free(elements);
    free(elements_1);
    free(elements_2);
}


/* Test ShBF_A */
void test_ShBF_A() {

    int i;
    int num_s1_only_elements = 100;
    int num_s2_only_elements = 100;
    int num_both_elements = 100;
    int num_elements = num_s1_only_elements + num_s2_only_elements + num_both_elements;
    char** elements;
    char** s1_elements;
    char** s2_elements;
    ShBF* test;
    int query_result;

    elements = generate_elements(num_elements);
    s1_elements = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * (num_s1_only_elements + num_both_elements));
    s2_elements = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * (num_s2_only_elements + num_both_elements));

    for (i = 0; i < num_s1_only_elements; i++) { s1_elements[i] = elements[i]; }
    for (i = 0; i < num_s2_only_elements; i++) { s2_elements[i] = elements[i + num_s1_only_elements]; }
    for (i = 0; i < num_both_elements; i++) {

        s1_elements[i + num_s1_only_elements] = elements[i + num_s1_only_elements + num_s2_only_elements];
        s1_elements[i + num_s2_only_elements] = elements[i + num_s1_only_elements + num_s2_only_elements];
    }

    test = new_ShBF_A(3000, num_elements);

    printf("===== BEFORE INSERTION ====\n");
    print_ShBF(test);

    for (i = 0; i < num_s1_only_elements; i++) { insert_ShBF_A(test, s1_elements[i], 1, 0); }
    for (i = 0; i < num_s2_only_elements; i++) { insert_ShBF_A(test, s2_elements[i], 0, 1); }
    for (i = 0; i < num_both_elements; i++)    { insert_ShBF_A(test, s1_elements[i + num_s1_only_elements], 1, 1); }

    printf("===== AFTER INSERTION =====\n");
    print_ShBF(test);

    printf("\n===== TESTING S1 ONLY ELEMENTS =====\n");    

    for (i = 0; i < num_s1_only_elements; i++) { 
    
        query_result = query_ShBF_A(test, s1_elements[i]); 

        if      (query_result == 6) { printf("In Union     - BAD!!!!\n"); }
        else if (query_result == 5) { printf("In S1        - BAD!!!!\n"); } 
        else if (query_result == 4) { printf("In S2        - BAD!!!!\n"); }
        else if (query_result == 3) { printf("Not in UNION - BAD!!!!\n"); } 
        else if (query_result == 2) { printf("Both\n"); } 
        else if (query_result == 1) { printf("S1 only\n"); }
        else if (query_result == 0) { printf("S2 only\n"); }
    }

    printf("\n===== TESTING S2 ONLY ELEMENTS =====\n");  

    for (i = 0; i < num_s2_only_elements; i++) { 

        query_result = query_ShBF_A(test, s2_elements[i]); 

        if      (query_result == 6) { printf("In Union     - BAD!!!!\n"); }
        else if (query_result == 5) { printf("In S1        - BAD!!!!\n"); } 
        else if (query_result == 4) { printf("In S2        - BAD!!!!\n"); }
        else if (query_result == 3) { printf("Not in UNION - BAD!!!!\n"); } 
        else if (query_result == 2) { printf("Both\n"); } 
        else if (query_result == 1) { printf("S1 only\n"); }
        else if (query_result == 0) { printf("S2 only\n"); }
    }

    printf("\n===== TESTING INTERSECTION ELEMENTS =====\n");  

    for (i = 0; i < num_both_elements; i++) { 
 
        query_result = query_ShBF_A(test, s1_elements[i + num_s1_only_elements]); 

        if      (query_result == 6) { printf("In Union     - BAD!!!!\n"); }
        else if (query_result == 5) { printf("In S1        - BAD!!!!\n"); } 
        else if (query_result == 4) { printf("In S2        - BAD!!!!\n"); }
        else if (query_result == 3) { printf("Not in UNION - BAD!!!!\n"); } 
        else if (query_result == 2) { printf("Both\n"); } 
        else if (query_result == 1) { printf("S1 only\n"); }
        else if (query_result == 0) { printf("S2 only\n"); }
    }

    free_ShBF(test);
    for (i = 0; i < num_elements; i++) { free(elements[i]); }
    free(elements);
    free(s1_elements);
    free(s2_elements);
}


/* Test ShBF_X */
void test_ShBF_X() {

    int i;
    int num_s_elements = 100;
    int num_r_elements = 100;
    char** s_elements;
    char** r_elements;
    ShBF* test;
    int query_result;
    int* counts;

    s_elements = generate_elements(num_s_elements);
    r_elements = generate_elements(num_r_elements);   

    counts = (int*)malloc(num_s_elements * sizeof(int));

    test = new_ShBF_X(1000, num_s_elements, 40);

    printf("===== BEFORE INSERTION ====\n");
    print_ShBF(test);

    for (i = 0; i < num_s_elements; i++) {
    
        counts[i] = (rand() % test->w) + 1;
        insert_ShBF_X(test, s_elements[i], counts[i]);        
    }

    printf("===== AFTER INSERTION =====\n");
    print_ShBF(test);
    
    printf("====== TESTING COUNTS ======\n");

    for (i = 0; i < num_s_elements; i++) {

        query_result = query_ShBF_X(test, s_elements[i]);

        printf("Result: %d Actual: %d\n", query_result, counts[i]);

        if (query_result < counts[i]) {

            printf("BAD!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
        }
    }

    printf("\n====== TESTING 0 COUNTS ======\n");

    for (i = 0; i < num_r_elements; i++) {

        query_result = query_ShBF_X(test, r_elements[i]);

        printf("Result: %d\n", query_result);
    }

    free(test);
    free(s_elements);
    free(r_elements);
}


/* Generates a set of n random elements */
char** generate_elements(int n) {

    char** random_elements;
    char* random_element;
    int size = 0;
    int duplicate;
    int i;
    
    random_elements = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * n);

    while (size < n) {

        //printf("Size: %d\n", size);

        random_element = generate_element();

        //duplicate = 0;
        //for (i = 0; i < size; i++) {

        //    if (!strcmp(random_element, random_elements[i])) {

        //        duplicate = 1;
        //        break;
        //    }
        //}

        //if (!duplicate) { 
        
            random_elements[size] = random_element;
            size += 1; 
        //}
    }

    return random_elements;
}


/* Generates a random 10 digit element */
char* generate_element() {

    char* random_element = (char*)malloc((ELEMENT_LENGTH + 1) * sizeof(char));
    int i;

    for (i = 0; i < ELEMENT_LENGTH; i++) {

        random_element[i] = (rand() % 10) + '0';
    }
    random_element[i] = '\0';

    return random_element;    
}
