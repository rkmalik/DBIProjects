#ifndef SORTED_DBFILE_H
#define SORTED_DBFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "BaseFile.h"
#include "BigQ.h"
#include <string>
#include <iostream>
#include <fstream>

class SortedDBFile : public BaseFile
{
private:
	int rwMode; //0 for reading,1 for writing
	File file;
	Page currentPage;
	off_t currentPageIndex;
	OrderMaker so;
	Pipe *inputPipe;
	Pipe *outputPipe;
	BigQ *bigQ;
	static const int pipeBufferSize = 100;
	int runLength;
	string filePath;

	int checkForFileMode();
	void ReleaseResources();
	void WriteToDisk();
	void MergeAllRecords();

public:
 	int g_return_value;	//1 for OK and -1 for error
  SortedDBFile ();
  int Create (char *fpath, fType file_type, void *startup);
  int Open (char *fpath);
  int Close ();
  void Load (Schema &myschema, char *loadpath);
  void MoveFirst ();
  void Add (Record &addme);
  int GetNext (Record &fetchme);
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif
