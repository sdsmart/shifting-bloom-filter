#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "MurmurHash3.h"

#define K_OPT_COEF 0.7009
#define MURMUR_HASH_SEED 304837963
#define ELEMENT_LENGTH 10
#define W 57

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
ShBF* new_ShBF_X(int m, int n, int max_x);
void free_ShBF(ShBF* shbf);
void print_ShBF(ShBF* shbf);
int get_bit_ShBF(ShBF* shbf, int index);
void set_bit_ShBF(ShBF* shbf, int index);
void insert_ShBF_M(ShBF* shbf_m, char* e);
int query_ShBF_M(ShBF* shbf_m, char* e);
void insert_ShBF_X(ShBF* shbf_x, char* e, int x);
int query_ShBF_X(ShBF* shbf_x, char* e);
char* generate_element();

/* Main  - Executes tests */
int main() {

    int i;
    int num_elements = 100;
    char* elements[num_elements];
    ShBF* test;
    int query_result;
    int in_set = 0;
    int out_of_set = num_elements;

    srand(time(NULL));

    test = new_ShBF_M(1000, 100);

    printf("===== BEFORE INSERTION ====\n");
    print_ShBF(test);

    for (i = 0; i < num_elements; i++) {
    
        elements[i] = generate_element();;
        insert_ShBF_M(test, elements[i]);        
     }

    printf("===== AFTER INSERTION =====\n");
    print_ShBF(test);

    for (i = 0; i < num_elements; i++) {
    
        query_result = query_ShBF_M(test, elements[i]);
        in_set += query_result;
        out_of_set -= query_result;
     }

    printf("===== TEST 1 RESULTS - QUERYING INSERTED ELEMENTS =====\n");
    printf("Number of elements in set     : %d\n", in_set);
    printf("Number of elements not in set : %d\n\n", out_of_set);

    in_set = 0;
    out_of_set = 1000000;    

    for (i = 0; i < 1000000; i++) {

        query_result = query_ShBF_M(test, generate_element());
        in_set += query_result;
        out_of_set -= query_result;
     }

    printf("===== TEST 2 RESULTS - QUERYING 1,000,000 RANDOM ELEMENTS =====\n");
    printf("Number of elements in set     : %d\n", in_set);
    printf("Number of elements not in set : %d\n\n", out_of_set);

    free_ShBF(test);
   
    return 0;
}

/* Constructor for shifitng bloom filter for set membership queries */
ShBF* new_ShBF_M(int m, int n) {

    ShBF* shbf;
    int k;
    unsigned char* B;
    int B_size;
    int i;

    k = (int)(K_OPT_COEF * ((float)m / n)) + 1;

    B_size = ((m % 8) == 0) ? m : ((m / 8) + 1) * 8;
    B_size += (W - 1);
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

/* Constructor for shifitng bloom filter for set multiplicity queries */
ShBF* new_ShBF_X(int m, int n, int max_x) {

    ShBF* shbf;
    int k;
    unsigned char* B;
    int B_size;
    int i;

    k = (int)(K_OPT_COEF * ((float)m / n)) + 1;

    B_size = (m + (max_x - 1));
    if ((B_size % 8) != 0) { ((B_size / 8) + 1) * 8; }
    
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

/* Destructor for general shifting bloom filter struct */
void free_ShBF(ShBF* shbf) {

    free(shbf->B);
    free(shbf);
}

/* Prints the contents of a genearl shifting bloom filter */
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
    printf("=============================\n\n");
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

/* Inserts an element into the ShBF for set membership queries */
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
    offset_value = (offset_hash % (W - 1)) + 1;

    for (i = 1; i <= shbf_m->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        first_index = i_hash % shbf_m->m;
        second_index = first_index + offset_value;
    
        set_bit_ShBF(shbf_m, first_index);
        set_bit_ShBF(shbf_m, second_index);
    }
}

/* Queries the ShBF for set membership queries to determine if a given element
   is present */
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
    offset_value = (offset_hash % (W - 1)) + 1;

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

/* Inserts an element into the ShBF for set multiplicity queries */
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

/* Queries the ShBF for set multiplicity queries to determine if a given element
   is present */
int query_ShBF_X(ShBF* shbf_x, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    uint64_t hash_indexes[shbf_x->k];
    int first_index;
    int second_index;
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

        if (ones_flag == 1) { multiplicity = (i + 1); }
    }

    return multiplicity;
}

/* Generates a random 10 digit element */
char* generate_element() {

    char* random_element;
    int i;

    random_element = (char*)malloc((ELEMENT_LENGTH + 1) * sizeof(char));
    
    for (i = 0; i < ELEMENT_LENGTH; i++) {

        random_element[i] = (rand() % 10) + '0';
    }
    random_element[i] = '\0';

    return random_element;    
}
