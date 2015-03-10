#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include <queue>
#include <time.h>

using namespace std;

class BigQ {

	File tempFile;
	Record myRecord;
	Page myPage;
	Page sortPage;
	int pageCounter;
	int runCounter;
	vector<Record *> myVector;
	vector<int> runL;

public:

	char tempFileName[10];
	Pipe *in;
	Pipe *out;
	OrderMaker orderMaker;
	int runLength;

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	BigQ();
	~BigQ ();
	void generateRuns(void);
	void mergeRuns(void);
	void sortAndWriteToFile(void);
	void writeToFile(void);
	void freeResourcesPhaseOne(void);
};

class CompareRecords{
    ComparisonEngine compEngine;
	OrderMaker* sortOrder;
public:
	CompareRecords(OrderMaker* sortorder)
	{
		sortOrder=sortorder;
	}
	bool operator()(Record *R1,Record *R2)
	{
		return compEngine.Compare(R1,R2,sortOrder)<0;
	}
};

class RunRecord{

public:
	Record record;
	int runNum;
};

class CompareQueues{
    ComparisonEngine compEngine;
	OrderMaker* sortOrder;
public:
	CompareQueues(OrderMaker *sortorder)
	{
		sortOrder=sortorder;
	}
	bool operator()(RunRecord *runR1,RunRecord *runR2)
	{
	    return compEngine.Compare(&(runR1->record),&(runR2->record),sortOrder)<0;
	}
};

#endif
