/* -------- COUNT MIN SKETCH ------- */
CREATE TYPE cms;

/* CMS basic functions */
CREATE FUNCTION cms_input(cstring)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_output(cms)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_receive(internal)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION cms_send(cms)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

/* CMS type definition */
CREATE TYPE cms (
    input = cms_input,
    output = cms_output,
    receive = cms_receive,
    send = cms_send,
    storage = extended
);

/* Create a new CMS */
CREATE FUNCTION new_cms(double precision default 0.001, double precision default 0.99)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	
/* Insert data into a CMS */    
CREATE FUNCTION insert_cms(cms, text)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
/* Query a CMS */
CREATE FUNCTION query_cms(cms, text)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* -------- BLOOM FILTER ------- */
CREATE TYPE bf;

/* CMS basic functions */
CREATE FUNCTION bf_input(cstring)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION bf_output(bf)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION bf_receive(internal)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION bf_send(bf)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

/* BF type definition */
CREATE TYPE bf (
    input = bf_input,
    output = bf_output,
    receive = bf_receive,
    send = bf_send,
    storage = extended
);

/* Create a new BF */
CREATE FUNCTION new_bf(integer, integer)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	   
/* Insert data into a BF */	   
CREATE FUNCTION insert_bf(bf, cstring)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	

/* Query a BF */
CREATE FUNCTION query_bf(bf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* -------- GENERAL SHIFTING BLOOM FILTER ------- */
CREATE TYPE shbf;

/* ShBF basic functions */
CREATE FUNCTION shbf_input(cstring)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION shbf_output(shbf)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION shbf_receive(internal)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION shbf_send(shbf)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

/* ShBF type definition */
CREATE TYPE shbf (
    input = shbf_input,
    output = shbf_output,
    receive = shbf_receive,
    send = shbf_send,
    storage = extended
);


/* Create a new ShBF for membership */
CREATE FUNCTION new_shbf_m(integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
/* Insert data into a ShBF_M */
CREATE FUNCTION insert_shbf_m(shbf, cstring)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	

/* Query a ShBF_M */
CREATE FUNCTION query_shbf_m(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* Create a new ShBF for association */
CREATE FUNCTION new_shbf_a(integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
/* Insert data into a ShBF_A */
CREATE FUNCTION insert_shbf_a(shbf, cstring, integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
/* Query a ShBF_A */
CREATE FUNCTION query_shbf_a(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* Create a new ShBF for multiplicity */
CREATE FUNCTION new_shbf_x(integer, integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
/* Insert data into a ShBF_X */
CREATE FUNCTION insert_shbf_x(shbf, cstring, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	

/* Query a ShBF_X */	
CREATE FUNCTION query_shbf_x(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

