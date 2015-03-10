#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <stdlib.h>

using namespace std;

SortedDBFile::SortedDBFile () : rwMode(0), filePath(), file(), currentPage(), currentPageIndex(0), runLength(100), so(), inputPipe(NULL), outputPipe(NULL), bigQ(NULL)
{
}


//Creates the DBFile for the Sorted File.
int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
  filePath = f_path;
  file.Open(0,f_path);
  return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
  rwMode = 1;
  if (NULL == bigQ) // initialize pipes, and BigQ
    {
      inputPipe = new Pipe(pipeBufferSize);
      outputPipe = new Pipe(pipeBufferSize);
      bigQ = new BigQ(*inputPipe,*outputPipe,so, runLength);
    }
  FILE *tableFile = fopen (loadpath, "r");
  if (0 == tableFile)
    exit(-1);
  Record tempRecord;
  int recordCounter = 0; // Debug counter
  //Inserting the records from the tbl files to the input pipes
  while (1 == tempRecord.SuckNextRecord (&f_schema, tableFile))
    {
      recordCounter++;
      if (recordCounter % 10000 == 0) {
        cerr << recordCounter << "\n";
      }
      inputPipe->Insert(&tempRecord);
    }
}

int SortedDBFile::Open (char *f_path) {
    int t;
    //Creating input stream for reading the metafile
    string metaFileName;
    metaFileName.append(f_path);
    metaFileName.append(".meta");
    ifstream metaFile;
    metaFile.open(metaFileName.c_str());
    if(!metaFile) return 1;
    metaFile >> t;
    if(!metaFile) return 1;
    //Reading data from the meta file and initializing the OrderMaker class
    metaFile >> runLength;
    metaFile >> so.numAtts;
	for (int i = 0; i < so.numAtts; i++) {
          metaFile >> so.whichAtts[i];
          int t;
          metaFile >> t;
          so.whichTypes[i] = (Type)t;
    }
    fType dbfileType = (fType) t;
    metaFile.close();
    //Initializing the gloabal filePath of the .bin file
    filePath = f_path;
    file.Open(1, f_path);
    rwMode = 0;//Opening in the ead/write mode
    MoveFirst();
    cout << "file opened with " << file.GetLength() << " pages" << endl;
    return 1;
}

/**
 * Method to move the pointer to the start of the file.
**/
void SortedDBFile::MoveFirst ()
{
	//If file in write mode, need to merge all contents to the file first.
	if(1 == rwMode)
		MergeAllRecords();

	//page index should be made 0, obviously
	currentPageIndex = (off_t)0;

	//current page should be the first page
	if(0 != file.GetLength())
		file.GetPage(&currentPage, currentPageIndex);
	else
		currentPage.EmptyItOut();
}

/**
 * Method to check the mode in which the db file is opened.
 * 1 if correct mode, 0 in case of error
**/
int SortedDBFile::checkForFileMode()
{
	//0 if opened in read mode, 1 if in write mode
	if((rwMode == 0) || (rwMode == 1))
		return 1;
	//Any other mode means error
	else
	{
		cout<<"Unknown file read write mode..."<<endl;
		return 0;
	}
}

/**
 * Method to release the memory allocations
**/
void SortedDBFile::ReleaseResources()
{
	delete inputPipe; inputPipe=NULL;
	delete outputPipe; outputPipe=NULL;
	delete bigQ; bigQ=NULL;
}

/**
 * Method to write the contents from output pipe into the disk
 * After writing is over, releasing the resources.
**/
void SortedDBFile::WriteToDisk()
{
	//If uninitialized, dont bother
	if(NULL == bigQ)
		return;

	//Close input pipe
	inputPipe->ShutDown();

	Record tempRecord;
	Page tempPage;

	//If more records are there...
	while(1 == outputPipe->Remove(&tempRecord))
	{
		//If current page is full
		if(0 == tempPage.Append(&tempRecord))
		{
			//Get the page counter to which this page is to be added
			off_t whichPage = (file.GetLength()>0) ? file.GetLength()-1 : 0;

			//Adding the page to the file
			file.AddPage(&tempPage, whichPage);

			//Initializing the page
			tempPage.EmptyItOut();

			//And adding the record inside it
			tempPage.Append(&tempRecord);
		}
	}

	//Adding the last page to the file
	off_t whichPage = (file.GetLength()>0) ? file.GetLength()-1 : 0;
	file.AddPage(&tempPage, whichPage);
	tempPage.EmptyItOut();

	//Writing done, now release the resources
	ReleaseResources();
}

/**
 * Method to close the db file.
 * 1 if closed properly, 0 otherwise
**/
int SortedDBFile::Close ()
{
	//First check if the file is opened in correct mode
	if(0 == checkForFileMode())
	{
		cout<<"Couldnt complete close.. exiting... "<<endl;
		return 0;
	}

	//For fresh file
	if(0 == file.GetLength())
	{
		//If in write mode, write everything to disk
		if(1 == rwMode)
			WriteToDisk();
	}

	//For already written file
	else
	{
		//If in write mode, merge the contents and write to disk
		if(1 == rwMode)
			MergeAllRecords();
	}

	//Try closing the file..
	int fileSize = file.Close();

	//If everything alright,
	if(fileSize >= 0)
		return 1;
	//Error logging
	else
	{
		cout<<"File size is negative. Cant close properly"<<endl;
		return 0;
	}
}

/**
 * This function simply adds the new record to the end of the file.
 * this function should actually consume addMe, so that after addMe has been
 * put into the file, it cannot be used again.
**/
void SortedDBFile::Add (Record &rec)
{
	//If the file is in read mode, then convert it into write mode first.
	if(0 == rwMode)
	{
		rwMode = 1;
		//Initialize bigQ if not done earlier
		if(NULL == bigQ)
		{
			inputPipe = new Pipe(pipeBufferSize);
			outputPipe = new Pipe(pipeBufferSize);
			bigQ = new BigQ(*inputPipe, *outputPipe, so, runLength);

		}
	}

	//If file is in write mode, then add the record into the input pipe
	if(rwMode==1)
	{
		inputPipe->Insert(&rec);
		g_return_value=1;
	}
}

/**
 * Function tp simply get the next record from the file and returns it to the user, where “next”
 * is defined to be relative to the current location of the pointer. After the function call
 * returns, the pointer into the file is incremented, so a subsequent call to GetNext won’t
 * return the same record twice. The return value is an integer whose value is zero if and
 * only if there is not a valid record returned from the function call (which will be the
 * case, for example, if the last record in the file has already been returned).
**/
int SortedDBFile::GetNext (Record &fetchme)
{
	//If file is in write mode, then need to merge all the
	//records before performing getnext function
	if(rwMode==1)
	{
		MergeAllRecords();
	}

	//If current page is not empty, then take out the first record
	if(1 == currentPage.GetFirst(&fetchme))
		return 1;

	//If current page is empty, then move to the next page
	currentPageIndex++;

	//If this was not the last page
	if((currentPageIndex+1) <= (file.GetLength()-1))
	{
		//Make the next page to be the current page
		file.GetPage(&currentPage, currentPageIndex);

		//Now take the first record out of the current page
		if(1 == currentPage.GetFirst(&fetchme))
			return 1;
	}
	//If this was the last page
	else
		return 0;

	//Error logging for this function
	cout<<"Unknown error in getnext function"<<endl;
	return 0;
}

/**
 * Function for getting the next record which returns the record which is satisfied by the
 * selection predicate. The literal record is used to check the selection predicate and is
 * created when the parse tree for the CNF is processed.
**/
int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
	//If file is in write mode, then need to merge all the
	//records before performing getnext function
	if(rwMode==1)
	{
		MergeAllRecords();
	}

	ComparisonEngine compEngine;
	//If there are more records
	while(1 == GetNext(fetchme))
	{
		//If the record fulfills the comparsion criteria
		if(compEngine.Compare(&fetchme, &literal, &cnf))
			return 1;
	}

	return 0;
}

/**
 * Method to merge the records which are already in
 * the file with those which are present in the bigQ pipes.
 * First insert all the records from original file to bigQ,
 * then write all the merged records inside the newly reopened file.
 * Once writing is done, move pointer till the first record.
**/
void SortedDBFile::MergeAllRecords(void)
{
	//Convert the file open mode to write mode
	rwMode = 0;

	//If fresh file, dont bother
	if(0 == file.GetLength())
		return;

	//Move to the first record
	MoveFirst();
	Record tempRecord;

	//While there are more records in the file,
	//keep inserting them into the pipe
	while(1 == GetNext(tempRecord))
		inputPipe->Insert(&tempRecord);

	//Close the current file..
	int fileSize = Close();
    char* f_path = (char*)filePath.c_str();
	//..and re-open file in create mode
	file.Open(0, f_path);

	//Write the contents from pipe to file
	WriteToDisk();

	//Move to the first record
	MoveFirst();

	//If we have come till here, means all is well
	g_return_value=1;
}
