#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "BaseFile.h"
#include "File.h"
#include "HeapDBFile.h"
#include "SortedDBFile.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
  BaseFile * genericFile;
public:
	DBFile ();
    int g_return_value;
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);


	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void setBaseFileNull(){
        this->genericFile = NULL;
	}
};


#endif

