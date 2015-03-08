# DBIProjects
===============


Team
====
- Name :  Rohit Kumar Malik
- UFID: 94711368
- Mail Id: rkmalik@ufl.edu


===========

- Name: Vignesh Miriyala
- UFId: 8819-3994
- Mail Id: vigneshmiriyala@ufl.edu


Instructions
============
Test driver for Assignment 1 heap DBFile (spring 2015 dbi)

This test driver gives a menu-based interface to three options that allows you to test your code:
- load (read a tpch file and write it out a heap DBFile)
- scan (read records from an existing heap DBFile)
- scan & filter (read records and filter using a CNF predicate)

Note that the driver only works with the tpch files (generated using the dbgen program).

To compile the driver, type

	make test1.out

To run the driver, type

	./test1.out
and follow the on-screen instructions.


select test:
 	 1. load file
 	 2. scan
 	 3. scan & filter


 select table:
	 1. nation
	 2. region
	 3. customer
	 4. part
	 5. partsupp
	 6. orders
	 7. lineitem


Case 1: Loads the Heap file from any of the 7 files.
Case 2: This scans the file for the records.
Case 3: Scans the record and filters the records based on the CNF Condition.


readme
=========
Test driver for Assignment 2 BigQ Milestone 1

This test driver gives a menu-based interface to three options that allows you
to test your code:

	1. sort
	2. sort + display
	3. sort + write

Note that the driver works only with heap dbfiles created over tpch tables.
Before using the driver, make sure you have generated the necessary heap dbfiles


To compile the driver, type
	make test.out

To run the driver, type
	./test.out
