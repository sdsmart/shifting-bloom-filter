#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <math.h>
#include "MurmurHash3.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/datum.h"
#include "utils/typcache.h"
#include "lib/stringinfo.h"
#include "utils/bytea.h"

#define MURMUR_HASH_SEED 304837963

#define K_OPT_BF 0.6931
#define K_OPT_SHBF_M 0.7009
#define K_OPT_SHBF_A 0.6931
#define K_OPT_SHBF_X 0.6931
#define W 57

#define ELEMENT_LENGTH 10

/* General bloom filter struct */
typedef struct BF {
    char vl_len_[4];
    int B_length;
    int m;
    int n;
    int k;
    unsigned char B[1];
} BF;

/* Count Min Sketch struct */
typedef struct CMS
{
    char vl_len_[4];
    uint32 sketchDepth;
    uint32 sketchWidth;
    uint64 sketch[1];
} CMS;

/* General shifting bloom filter struct */
typedef struct ShBF {
    char vl_len_[4];
    int B_length;
    int m;
    int n;
    int k;
    int w;
    unsigned char B[1];
} ShBF;

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(new_bf);
PG_FUNCTION_INFO_V1(insert_bf);
PG_FUNCTION_INFO_V1(query_bf);
PG_FUNCTION_INFO_V1(bf_input);
PG_FUNCTION_INFO_V1(bf_output);
PG_FUNCTION_INFO_V1(bf_receive);
PG_FUNCTION_INFO_V1(bf_send);

PG_FUNCTION_INFO_V1(new_cms);
PG_FUNCTION_INFO_V1(insert_cms);
PG_FUNCTION_INFO_V1(query_cms);
PG_FUNCTION_INFO_V1(cms_input);
PG_FUNCTION_INFO_V1(cms_output);
PG_FUNCTION_INFO_V1(cms_receive);
PG_FUNCTION_INFO_V1(cms_send);

/* Postgres function prototypes */
PG_FUNCTION_INFO_V1(new_shbf_m);
PG_FUNCTION_INFO_V1(insert_shbf_m);
PG_FUNCTION_INFO_V1(query_shbf_m);
PG_FUNCTION_INFO_V1(shbf_input);
PG_FUNCTION_INFO_V1(shbf_output);
PG_FUNCTION_INFO_V1(shbf_receive);
PG_FUNCTION_INFO_V1(shbf_send);

/* Function prototypes */
BF* new_BF(int m, int n);
void insert_BF(BF* bf, char* e);
int query_BF(BF* bf, char* e);

CMS* new_CMS(float8 errorBound, float8 confidenceInterval);
CMS* insert_CMS(CMS* currentCms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
uint64 query_CMS(CMS* cms, Datum item, TypeCacheEntry* itemTypeCacheEntry);

ShBF* new_ShBF_M(int m, int n);
void insert_ShBF_M(ShBF* shbf_m, char* e);
int query_ShBF_M(ShBF* shbf_m, char* e);

ShBF* new_ShBF_A(int m, int n);
void insert_ShBF_A(ShBF* shbf_a, char* e, int s1, int s2);
int query_ShBF_A(ShBF* shbf_a, char* e);

ShBF* new_ShBF_X(int m, int n, int max_x);
void insert_ShBF_X(ShBF* shbf_x, char* e, int x);
int query_ShBF_X(ShBF* shbf_x, char* e);

int get_bit_BF(BF* bf, int index);
void set_bit_BF(BF* bf, int index);
void print_BF(BF* bf);
void free_BF(BF* bf);

int get_bit_ShBF(ShBF* shbf, int index);
void set_bit_ShBF(ShBF* shbf, int index);
void print_ShBF(ShBF* shbf);
void free_ShBF(ShBF* shbf);

uint64 update_cms_in_place(CMS* cms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
void convert_datum_to_bytes(Datum datum, TypeCacheEntry* datumTypeCacheEntry, StringInfo datumString);
uint64 estimate_hashed_item_frequency(CMS* cms, uint64* hashValueArray);

void test_BF(void);
void test_iBF(void);
void test_ShBF_M(void);
void test_ShBF_A(void);
void test_ShBF_X(void);

char** generate_elements(int n);
char* generate_element(void);


/*
 * --------------------------
 * --- Postgres Functions ---
 * --------------------------
*/


/* --- Bloom Filter Functions --- */


/* TODO */
Datum new_bf(PG_FUNCTION_ARGS) {

    int m = PG_GETARG_INT32(0);
    int n = PG_GETARG_INT32(1);

    BF* bf = new_BF(m, n);

    print_BF(bf);

    PG_RETURN_POINTER(bf);
}


/* TODO */
Datum insert_bf(PG_FUNCTION_ARGS) {

    BF* bf = NULL;
    char* new_item = 0;
    
    bf = (BF*) PG_GETARG_VARLENA_P(0);
    
    new_item = PG_GETARG_CSTRING(1);

    insert_BF(bf, new_item);

    PG_RETURN_POINTER(bf);
}


/* TODO */
Datum query_bf(PG_FUNCTION_ARGS) {

    BF* bf = (BF*) PG_GETARG_VARLENA_P(0);
    char* item = PG_GETARG_CSTRING(1);
    int result = 0;

    result = query_BF(bf, item);
 
    PG_RETURN_INT32(result);
}


/* TODO */
Datum bf_input(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum bf_output(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/* TODO */
Datum bf_receive(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum bf_send(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/* --- Count Min Sketch Fuctions --- */


/* TODO */
Datum new_cms(PG_FUNCTION_ARGS) {

    float8 errorBound = PG_GETARG_FLOAT8(0);
    float8 confidenceInterval =  PG_GETARG_FLOAT8(1);

    CMS* cms = new_CMS(errorBound, confidenceInterval);

    PG_RETURN_POINTER(cms);
}


/* TODO */
Datum insert_cms(PG_FUNCTION_ARGS) {

    CMS* currentCms = NULL;
    CMS* updatedCms = NULL;
    Datum newItem = 0;
    TypeCacheEntry* newItemTypeCacheEntry = NULL;
    Oid newItemType = InvalidOid;

    /* Check whether cms is null */
    if (PG_ARGISNULL(0))
    {
        PG_RETURN_NULL();
    }
    else
    {
        currentCms = (CMS*) PG_GETARG_VARLENA_P(0);
    }

    /* If new item is null, then return current CountMinSketch */
    if (PG_ARGISNULL(1))
    {
        PG_RETURN_POINTER(currentCms);
    }

    /* Get item type and check if it is valid */
    newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (newItemType == InvalidOid)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }

    newItem = PG_GETARG_DATUM(1);
    newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);
    updatedCms = insert_CMS(currentCms, newItem, newItemTypeCacheEntry);

    PG_RETURN_POINTER(updatedCms);
}


/* TODO */
Datum query_cms(PG_FUNCTION_ARGS) {

    CMS* cms = (CMS*) PG_GETARG_VARLENA_P(0);
    Datum item = PG_GETARG_DATUM(1);
    Oid itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    TypeCacheEntry *itemTypeCacheEntry = NULL;
    uint64 frequency = 0;

    if (itemType == InvalidOid)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data types")));
    }

    itemTypeCacheEntry = lookup_type_cache(itemType, 0);
    frequency = query_CMS(cms, item, itemTypeCacheEntry);

    PG_RETURN_INT32(frequency);
}


/* TODO */
Datum cms_input(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum cms_output(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/* TODO */
Datum cms_receive(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum cms_send(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/* --- Shifting Bloom Filter Functions --- */


/* TODO */
Datum new_shbf_m(PG_FUNCTION_ARGS) {
    int m = PG_GETARG_INT32(0);
    int n = PG_GETARG_INT32(1);

    ShBF* shbf_m = new_ShBF_M(m, n);

    print_ShBF(shbf_m);

    PG_RETURN_POINTER(shbf_m);
}


/* TODO */
Datum insert_shbf_m(PG_FUNCTION_ARGS) {

    ShBF* shbf_m = NULL;
    char* new_item = 0;
    
    shbf_m = (ShBF*) PG_GETARG_VARLENA_P(0);
    
    new_item = PG_GETARG_CSTRING(1);

    insert_ShBF_M(shbf_m, new_item);

    PG_RETURN_POINTER(shbf_m);
}


/* TODO */
Datum query_shbf_m(PG_FUNCTION_ARGS) {

    ShBF* shbf_m = (ShBF*) PG_GETARG_VARLENA_P(0);
    char* item = PG_GETARG_CSTRING(1);
    int result = 0;

    result = query_ShBF_M(shbf_m, item);
 
    PG_RETURN_INT32(result);
}


/* TODO */
Datum shbf_input(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum shbf_output(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/* TODO */
Datum shbf_receive(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/* TODO */
Datum shbf_send(PG_FUNCTION_ARGS) {

	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/*
 * -------------------
 * --- C Functions ---
 * -------------------
*/ 


/* --- Bloom Filter Functions --- */


/* Constructor for BF */
BF* new_BF(int m, int n) {

    BF* bf;
    int k;
    int k_floor;
    float k_unrounded;
    int B_size;
    Size bf_size;

    k_unrounded = (K_OPT_BF * ((float)m / n));
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = ((m % 8) == 0) ? (m / 8) : ((m / 8) + 1);

    bf_size = B_size + sizeof(BF);

    bf = palloc0(bf_size);
    bf->B_length = B_size;
    bf->m = m;
    bf->n = n;
    bf->k = k;

    SET_VARSIZE(bf, bf_size);

    return bf;
}


/* Inserts an element into BF */
void insert_BF(BF* bf, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    int index;
    int i;

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

    for (i = 0; i < bf->k; i++) { 
        
        i_hash = hva[0] + i*hva[1];

        index = i_hash % bf->m;
   
        set_bit_BF(bf, index);
    }
}


/* Queries BF to determine if a given element is present */
int query_BF(BF* bf, char* e) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    int index;
    int i;

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

    for (i = 0; i < bf->k; i++) { 
 
        i_hash = hva[0] + i*hva[1];

        index = i_hash % bf->m;
    
        if (get_bit_BF(bf, index) != 1) { return 0; }
    } 

    //print_BF(bf);

    return 1;
}


/* --- Count Min Sketch Functions --- */


/* TODO */
CMS* new_CMS(float8 errorBound, float8 confidenceInterval) {

    CMS* cms = NULL;
    uint32 sketchWidth = 0;
    uint32 sketchDepth = 0;
    Size staticStructSize = 0;
    Size sketchSize = 0;
    Size totalCmsSize = 0;
    
    if (errorBound <= 0 || errorBound >= 1)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid parameters for cms"),
                        errhint("Error bound has to be between 0 and 1")));
    }
    else if (confidenceInterval <= 0 || confidenceInterval >= 1)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid parameters for cms"),
                        errhint("Confidence interval has to be between 0 and 1")));
    }

    sketchWidth = (uint32) ceil(exp(1) / errorBound);
    sketchDepth = (uint32) ceil(log(1 / (1 - confidenceInterval)));
    sketchSize =  sizeof(uint64) * sketchDepth * sketchWidth;
    staticStructSize = sizeof(CMS);
    totalCmsSize = staticStructSize + sketchSize;

    cms = palloc0(totalCmsSize);
    cms->sketchDepth = sketchDepth;
    cms->sketchWidth = sketchWidth;

    SET_VARSIZE(cms, totalCmsSize);

    return cms;
}


/* TODO */
CMS* insert_CMS(CMS* currentCms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry) {

    Datum detoastedItem = 0;

    /* If datum is toasted, detoast it */
    if (newItemTypeCacheEntry->typlen == -1)
    {
        detoastedItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
    }
    else
    {
        detoastedItem = newItem;
    }

    update_cms_in_place(currentCms, detoastedItem, newItemTypeCacheEntry);

    return currentCms;
}


/* TODO */
uint64 query_CMS(CMS* cms, Datum item, TypeCacheEntry* itemTypeCacheEntry) {

	uint64 hashValueArray[2] = {0, 0};
	StringInfo itemString = makeStringInfo();
	uint64 frequency = 0;

	/* If datum is toasted, detoast it */
	if (itemTypeCacheEntry->typlen == -1)
	{
		Datum detoastedItem =  PointerGetDatum(PG_DETOAST_DATUM(item));
		convert_datum_to_bytes(detoastedItem, itemTypeCacheEntry, itemString);
	}
	else
	{
		convert_datum_to_bytes(item, itemTypeCacheEntry, itemString);
	}

	/*
	 * Calculate hash values for the given item and then get frequency estimate
	 * with these hashed values.
	 */
	MurmurHash3_x64_128(itemString->data, itemString->len, MURMUR_HASH_SEED, &hashValueArray);
	frequency = estimate_hashed_item_frequency(cms, hashValueArray);

	return frequency;
}


/* TODO */
uint64 update_cms_in_place(CMS* cms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry) {

    uint32 hashIndex = 0;
    uint64 hashValueArray[2] = {0, 0};
    StringInfo newItemString = makeStringInfo();
    uint64 newFrequency = 0;
    uint64 minFrequency = UINT64_MAX;

    /* Get hashed values for the given item */
    convert_datum_to_bytes(newItem, newItemTypeCacheEntry, newItemString);
    MurmurHash3_x64_128(newItemString->data, newItemString->len, MURMUR_HASH_SEED,
                        &hashValueArray);

    /*
     * Estimate frequency of the given item from hashed values and calculate new
     * frequency for this item.
     */
    minFrequency = estimate_hashed_item_frequency(cms, hashValueArray);
    newFrequency = minFrequency + 1;

    /*
     * We can create an independent hash function for each index by using two hash
     * values from the Murmur Hash function. This is a standard technique from the
     * hashing literature for the additional hash functions of the form
     * g(x) = h1(x) + i * h2(x) and does not hurt the independence between hash
     * function. For more information you can check this paper:
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    for (hashIndex = 0; hashIndex < cms->sketchDepth; hashIndex++)
    {
        uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
        uint32 widthIndex = hashValue % cms->sketchWidth;
        uint32 depthOffset = hashIndex * cms->sketchWidth;
        uint32 counterIndex = depthOffset + widthIndex;

        /*
         * Selective update to decrease effect of collisions. We only update
         * counters less than new frequency because other counters are bigger
         * due to collisions.
         */
        uint64 counterFrequency = cms->sketch[counterIndex];
        if (newFrequency > counterFrequency)
        {
            cms->sketch[counterIndex] = newFrequency;
        }
    }

    return newFrequency;
}


/* TODO */
void convert_datum_to_bytes(Datum datum, TypeCacheEntry* datumTypeCacheEntry, StringInfo datumString) {

    int16 datumTypeLength = datumTypeCacheEntry->typlen;
    bool datumTypeByValue = datumTypeCacheEntry->typbyval;
    Size datumSize = 0;

    if (datumTypeLength == -1)
    {
        datumSize = VARSIZE_ANY_EXHDR(DatumGetPointer(datum));
    }
    else
    {
        datumSize = datumGetSize(datum, datumTypeByValue, datumTypeLength);
    }

    if (datumTypeByValue)
    {
        appendBinaryStringInfo(datumString, (char *) &datum, datumSize);
    }
    else
    {
        appendBinaryStringInfo(datumString, VARDATA_ANY(datum), datumSize);
    }
}


/* TODO */
uint64 estimate_hashed_item_frequency(CMS* cms, uint64* hashValueArray) {

    uint32 hashIndex = 0;
    uint64 minFrequency = UINT64_MAX;

    for (hashIndex = 0; hashIndex < cms->sketchDepth; hashIndex++)
    {
        uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
        uint32 widthIndex = hashValue % cms->sketchWidth;
        uint32 depthOffset = hashIndex * cms->sketchWidth;
        uint32 counterIndex = depthOffset + widthIndex;

        uint64 counterFrequency = cms->sketch[counterIndex];
        if (counterFrequency < minFrequency)
        {
            minFrequency = counterFrequency;
        }
    }

    return minFrequency;
}


/* --- Shifting Bloom Filter Functions --- */


/* Constructor for ShBF_M */
ShBF* new_ShBF_M(int m, int n) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    int B_size;
    Size shbf_size;

    k_unrounded = K_OPT_SHBF_M * ((float)m / n) / 2;
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = m + (W - 1);
    B_size = ((B_size % 8) == 0) ? (B_size / 8) : ((B_size / 8) + 1);

    shbf_size = B_size + sizeof(ShBF);

    shbf = palloc0(shbf_size);
    shbf->B_length = B_size;
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = W;

    SET_VARSIZE(shbf, shbf_size);

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

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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

    
    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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

    //print_ShBF(shbf_m);

    return 1;
}

/* Constructor for ShBF_A */
ShBF* new_ShBF_A(int m, int n) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    int B_size;
    Size shbf_size;

    k_unrounded = K_OPT_SHBF_A * ((float)m / n);
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = m + (W - 2);
    B_size = ((B_size % 8) == 0) ? (B_size / 8) : ((B_size / 8) + 1);

    shbf_size = B_size + sizeof(ShBF);

    shbf = palloc0(shbf_size);
    shbf->B_length = B_size;
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = W;

    SET_VARSIZE(shbf, shbf_size);

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
    int offset_value = 0;
    int index;
    int i;

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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

    return -1;
}


/* Constructor for ShBF_X */
ShBF* new_ShBF_X(int m, int n, int max_x) {

    ShBF* shbf;
    int k;
    int k_floor;
    float k_unrounded;
    int B_size;
    Size shbf_size;

    k_unrounded = K_OPT_SHBF_X * ((float)m / n);
    k_floor = (int)k_unrounded;
    k = ((k_unrounded - k_floor) >= 0.5) ? (k_floor + 1) : (k_floor);
    if (k == 0) { k = 1; }

    B_size = (m + (max_x - 1));
    B_size = ((B_size % 8) == 0) ? (B_size / 8) : ((B_size / 8) + 1);

    shbf_size = B_size + sizeof(ShBF);

    shbf = palloc0(shbf_size);
    shbf->B_length = B_size;
    shbf->m = m;
    shbf->n = n;
    shbf->k = k;
    shbf->w = max_x;

    SET_VARSIZE(shbf, shbf_size);

    return shbf;
}


/* Inserts an element into ShBF_X */
void insert_ShBF_X(ShBF* shbf_x, char* e, int x) {

    uint64_t hva[2] = {0, 0};
    uint64_t i_hash;
    int offset_value;
    int index;
    int i;

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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

    MurmurHash3_x64_128(e, strlen(e), MURMUR_HASH_SEED, &hva);

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


/* Gets the bit at the specified index in the BF array */
int get_bit_BF(BF* bf, int index) {

    int B_index;
    int byte_offset;
    unsigned char byte;    
    int bit;

    B_index = index / 8;
    byte_offset = index % 8;

    byte = bf->B[B_index]; 
    bit = (byte >> (7 - byte_offset)) & 1;

    return bit;
}


/* Sets the bit at the specified index in the BF array to 1 */
void set_bit_BF(BF* bf, int index) {

    int B_index;
    int byte_offset;
    unsigned char mask = 128;    

    B_index = index / 8;
    byte_offset = index % 8;

    mask >>= byte_offset;
    bf->B[B_index] = bf->B[B_index] | mask;
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

    B_index = index / 8;
    byte_offset = index % 8;

    mask >>= byte_offset;
    shbf->B[B_index] = shbf->B[B_index] | mask;
}

/* Prints the contents of a general bloom filter */
void print_BF(BF* bf) {

    int ones;
    int zeros;
    int bit;
    int i;    

    ones = 0;
    zeros = 0;
    for (i = 0; i < bf->B_length*8; i++) { 
        
        bit = get_bit_BF(bf, i);

        if (bit == 0) { zeros++; }
        else if (bit == 1) { ones++; }
        else { printf("ERROR - BIT NOT 0 or 1"); }
    }

    //ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("")));

    printf("======= BF Contents =======\n");
    printf("Number of 1's: %d\n", ones);
    printf("Number of 0's: %d\n", zeros);
    printf("B_length: %d\nm: %d\nn: %d\nk: %d\n", bf->B_length, bf->m, 
                                                  bf->n, bf->k);
    printf("===========================\n\n");
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

    //ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("")));
    
    printf("====== ShBF Contents ======\n");
    printf("Number of 1's: %d\n", ones);
    printf("Number of 0's: %d\n", zeros);
    printf("B_length: %d\nm: %d\nn: %d\nk: %d\n", shbf->B_length, shbf->m, 
                                                  shbf->n, shbf->k);
    printf("===========================\n\n");
}


/* Destructor for general bloom filter struct */
void free_BF(BF* bf) {

    free(bf->B);
    free(bf);
}


/* Destructor for general shifting bloom filter struct */
void free_ShBF(ShBF* shbf) {

    free(shbf->B);
    free(shbf);
}


/* 
 * ----------------------
 * --- Test Functions ---
 * ----------------------
*/  


/* Test BF */
void test_BF() {

    int i;
    int num_elements_1 = 100;
    int num_elements_2 = 1000000;
    int num_elements = num_elements_1 + num_elements_2;
    char** elements;
    char** elements_1;
    char** elements_2;
    BF* test;
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

    test = new_BF(1285, num_elements_1);

    printf("===== BEFORE INSERTION ====\n");
    print_BF(test);

    for (i = 0; i < num_elements_1; i++) {
    
        insert_BF(test, elements_1[i]);        
    }

    printf("===== AFTER INSERTION =====\n");
    print_BF(test);

    for (i = 0; i < num_elements_1; i++) {
    
        query_result = query_BF(test, elements_1[i]);
        in_set += query_result;
        out_of_set -= query_result;
    }

    printf("===== TEST 1 RESULTS - QUERYING INSERTED ELEMENTS =====\n");
    printf("Number of elements in set     : %d\n", in_set);
    printf("Number of elements not in set : %d\n\n", out_of_set);

    for (i = 0; i < num_elements_2; i++) {

        query_result = query_BF(test, elements_2[i]);

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

    free_BF(test);
    for (i = 0; i < num_elements; i++) { free(elements[i]); }
    free(elements);
    free(elements_1);
    free(elements_2);
}


/* Test iBF */
void test_iBF() {

    int i;
    int num_s1_only_elements = 100;
    int num_s2_only_elements = 100;
    int num_both_elements = 100;
    int num_elements = num_s1_only_elements + num_s2_only_elements + num_both_elements;
    int num_s1_elements = num_s1_only_elements + num_both_elements;
    int num_s2_elements = num_s2_only_elements + num_both_elements;
    char** elements;
    char** s1_elements;
    char** s2_elements;
    BF* test_s1;
    BF* test_s2;
    int query_result_s1;
    int query_result_s2;

    elements = generate_elements(num_elements);
    s1_elements = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * num_s1_elements);
    s2_elements = (char**)malloc(((ELEMENT_LENGTH + 1) * sizeof(char)) * num_s2_elements);

    for (i = 0; i < num_s1_only_elements; i++) { s1_elements[i] = elements[i]; }
    for (i = 0; i < num_s2_only_elements; i++) { s2_elements[i] = elements[i + num_s1_only_elements]; }
    for (i = 0; i < num_both_elements; i++) {

        s1_elements[i + num_s1_only_elements] = elements[i + num_s1_only_elements + num_s2_only_elements];
        s2_elements[i + num_s2_only_elements] = elements[i + num_s1_only_elements + num_s2_only_elements];
    }

    test_s1 = new_BF(1000, num_s1_elements);
    test_s2 = new_BF(1000, num_s2_elements); 

    printf("===== BEFORE INSERTION ====\n");
    print_BF(test_s1);
    print_BF(test_s2);

    for (i = 0; i < num_s1_elements; i++) { insert_BF(test_s1, s1_elements[i]); }
    for (i = 0; i < num_s2_elements; i++) { insert_BF(test_s2, s2_elements[i]); }

    printf("===== AFTER INSERTION =====\n");
    print_BF(test_s1);
    print_BF(test_s2);

    printf("\n===== TESTING S1 ONLY ELEMENTS =====\n");    

    for (i = 0; i < num_s1_only_elements; i++) { 

        query_result_s1 = query_BF(test_s1, s1_elements[i]);
        query_result_s2 = query_BF(test_s2, s1_elements[i]);

        if      (query_result_s1 && query_result_s2) { printf("Both       - BAD!!!!\n"); }
        else if (query_result_s1)                    { printf("S1 only\n"); }
        else if (query_result_s2)                    { printf("S2 only    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
        else                                         { printf("Neither    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
    }

    printf("\n===== TESTING S2 ONLY ELEMENTS =====\n");  

    for (i = 0; i < num_s2_only_elements; i++) { 

        query_result_s1 = query_BF(test_s1, s2_elements[i]); 
        query_result_s2 = query_BF(test_s2, s2_elements[i]); 

        if      (query_result_s1 && query_result_s2) { printf("Both       - BAD!!!!\n"); }
        else if (query_result_s1)                    { printf("S1 only    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
        else if (query_result_s2)                    { printf("S2 only\n"); }
        else                                         { printf("Neither    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
    }

    printf("\n===== TESTING INTERSECTION ELEMENTS =====\n");  

    for (i = 0; i < num_both_elements; i++) { 
 
        query_result_s1 = query_BF(test_s1, s1_elements[i + num_s1_only_elements]);
        query_result_s2 = query_BF(test_s2, s1_elements[i + num_s1_only_elements]);

        if      (query_result_s1 && query_result_s2) { printf("Both\n"); }
        else if (query_result_s1)                    { printf("S1 only    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
        else if (query_result_s2)                    { printf("S2 only    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
        else                                         { printf("Neither    - WRONG - REALLY BAD!!!!!!!!!!\n"); }
    }

    free_BF(test_s1);
    free_BF(test_s2);
    for (i = 0; i < num_elements; i++) { free(elements[i]); }
    free(elements);
    free(s1_elements);
    free(s2_elements);
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
        s2_elements[i + num_s2_only_elements] = elements[i + num_s1_only_elements + num_s2_only_elements];
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
        else if (query_result == 2) { printf("Both         - WRONG - REALLY BAD!!!!!!!!!!\n"); } 
        else if (query_result == 1) { printf("S1 only\n"); }
        else if (query_result == 0) { printf("S2 only      - WRONG - REALLY BAD!!!!!!!!!!\n"); }
    }

    printf("\n===== TESTING S2 ONLY ELEMENTS =====\n");  

    for (i = 0; i < num_s2_only_elements; i++) { 

        query_result = query_ShBF_A(test, s2_elements[i]); 

        if      (query_result == 6) { printf("In Union     - BAD!!!!\n"); }
        else if (query_result == 5) { printf("In S1        - BAD!!!!\n"); } 
        else if (query_result == 4) { printf("In S2        - BAD!!!!\n"); }
        else if (query_result == 3) { printf("Not in UNION - BAD!!!!\n"); } 
        else if (query_result == 2) { printf("Both         - WRONG - REALLY BAD!!!!!!!!!!\n"); } 
        else if (query_result == 1) { printf("S1 only      - WRONG - REALLY BAD!!!!!!!!!!\n"); }
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
        else if (query_result == 1) { printf("S1 only      - WRONG - REALLY BAD!!!!!!!!!!\n"); }
        else if (query_result == 0) { printf("S2 only      - WRONG - REALLY BAD!!!!!!!!!!\n"); }
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
    //int duplicate;
    //int i;
    
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
