#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H
#include "DBFile.h"
#include "BaseFile.h"



class SortedDBFile: public BaseFile {



public:
	SortedDBFile ();
	~SortedDBFile();

	int Create (char* fpath, void* startup);
	void Load (Schema &mySchema, char *loadMe);
	int GetNext (Record& fetchme);

	int GetNext (Record &fetchMe, CNF &applyMe, Record &literal);



	void Add (Record& me);
	void AddPage();
	void MoveFirst();
};





#endif
