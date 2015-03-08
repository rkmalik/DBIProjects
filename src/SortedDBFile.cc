#include <iostream> // only for debugging
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () {

}
SortedDBFile::~SortedDBFile() {

}


int SortedDBFile::Create (char* fpath, void* startup)
{
    file.Open(0, fpath);
	return 1;
}
void SortedDBFile::Load (Schema &mySchema, char *loadMe)
{


}
int SortedDBFile::GetNext (Record& fetchme)
{


    return 0;
}

int SortedDBFile::GetNext (Record &fetchMe, CNF &applyMe, Record &literal)
{

    return 0;
}



void SortedDBFile::Add (Record& me)
{

}
void SortedDBFile::AddPage()
{


}
void SortedDBFile::MoveFirst()
{


}



