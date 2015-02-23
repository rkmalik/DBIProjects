#include <fstream>
#include <iostream>
#include "DBFile.h"
#include "HeapDBFile.h"
#include "BaseFile.h"


using namespace std;

BaseFile::BaseFile()
{
	index = -1;
	totalPageCount = 0;

}
BaseFile::~BaseFile()
{


}
int BaseFile::Open (char* fpath) {
	cout << "Opening the file  " <<  fpath << endl;
	file.Open(1, fpath);
	return 1;
}

int BaseFile::Close ()
{
    page.EmptyItOut ();
	file.Close ();
	return 0;
}

void BaseFile::GetPage (Page *putItHere, off_t whichPage)
{
    file.GetPage(putItHere, whichPage);
}


