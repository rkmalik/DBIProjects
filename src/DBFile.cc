#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string>
#include <stdlib.h>
#include <iostream>
#include <fstream>
// stub file .. replace it with your own DBFile.cc

using namespace std;
/*
 *Default Constructor.
 */
DBFile::DBFile () {
    g_return_value=1;
}

/*
 *This method is used to create an instance of a heap file.
 */
int DBFile::Create (char *file_path, fType file_type, void *startup) {
    if(file_path == NULL)
        return 0;
    if(file_type != heap && file_type != sorted)
        return 0;
    string s = (string) file_path;
    if(s.substr(s.find_last_of(".") + 1) != "bin" && s.substr(s.find_last_of(".") + 1) != "bigq"){
        return 0;
    }
    if(startup == NULL && file_type == sorted)
        return 0;
    //Creating the meta file
    string meta_file_Name;
    meta_file_Name.append(file_path);
    meta_file_Name.append(".meta");
    ofstream meta_file;
    meta_file.open(meta_file_Name.c_str());
    meta_file << file_type << endl; // write db type to the meta file
    //Writing contents of the db type to meta file
    if(sorted == file_type)
      {
        SortInfo si = *((SortInfo *)startup);
        meta_file << si.runLength << endl;
        OrderMaker om = *(OrderMaker *)si.myOrder;
        meta_file << om.numAtts << endl;
        for (int i = 0; i < om.numAtts; i++) {
          meta_file << om.whichAtts[i] << " ";
          meta_file << om.whichTypes[i] << endl;
        }
      }
    meta_file.close();

    //End of the meta file block
    switch(file_type)
    {
    case heap:// If the file is of type heap
      cout << "Opening in heap file mode." <<  endl;
      genericFile = new HeapDBFile();
      break;
    case sorted:
      cout << "create a sorted dbfile" << endl;
      genericFile = new SortedDBFile();
      break;
    default:
      cout << "File Type not identified." <<  endl;
      exit(-1);
    }
  //Call to the create method of the generic version of the file
  return genericFile->Create(file_path, file_type, startup);
}

int DBFile::Open (char *file_path) {
    if(file_path == NULL)
        return 0;
    string s = (string) file_path;
    if(s.substr(s.find_last_of(".") + 1) != "bin"){
        return 0;
    }
    //Reading File Type from meta file
    int type;
    string meta_file_Name;
    meta_file_Name.append(file_path);
    meta_file_Name.append(".meta");
    ifstream meta_file;
    meta_file.open(meta_file_Name.c_str());
    meta_file >> type;
    fType db_file_Type = (fType) type;
    meta_file.close();
    //Closing the meta file
    switch(db_file_Type){
        case heap:
            cout << "Opening a heaped dbfile" << endl;
            genericFile = new HeapDBFile();
            break;
        case sorted: // fall through, not implemented
            cout << "Opening a sorted dbfile" << endl;
            genericFile = new SortedDBFile();
            break;
        default:
            cout << "Unidentified File Type" <<  endl;
            exit(-1);
    }
    return genericFile->Open(file_path);
}


void DBFile::Load (Schema &f_schema, char *loadpath) {
    //Check for NULL load path
    if(loadpath == NULL){
        cout << "Load Path is NULL"<<endl;
        g_return_value = 0;
        return;
    }
    //Check for Schema value to be NULL
    if(!&f_schema){
        g_return_value = 0;
        return;
    }
    //check for valid load path
    FILE *tableFile = fopen (loadpath, "r");
    if (0 == tableFile){
        g_return_value = 0;
        return;
    }
    if(genericFile == NULL){
        g_return_value = 0;
        return;
    }
    string s = (string) loadpath;
    if(s.substr(s.find_last_of(".") + 1) != "tbl"){
        g_return_value = 0;
        return;
    }
    genericFile->Load(f_schema,loadpath);
}


/*
 * This function forces the pointer to correspond to the first record in the file.
 */
void DBFile::MoveFirst () {
    if(genericFile == NULL){
        g_return_value = -1;
        return;
    }
    genericFile->MoveFirst();
}

int DBFile::Close () {
    int output = genericFile->Close();
    return output;
}

void DBFile::Add (Record &rec) {
    //Check for null record
    if(!&rec){
        g_return_value = -1;
        return;
    }
    genericFile->Add(rec);
}

int isValidRecord(Record &record)
{
    if(!&record)
        return 0;
    return 1;
}

int DBFile::GetNext (Record &fetchme) {
    if(isValidRecord(fetchme)==0)
        return 0;
    return genericFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(isValidRecord(fetchme)==0)
        return 0;
    if(!&cnf)
        return 0;

    if(!&literal)
        return 0;

    return genericFile->GetNext(fetchme, cnf, literal);
}
