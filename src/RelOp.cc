#include "RelOp.h"


/***************Implementation for Select File **************/

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


/***************Implementation for Select Pipe**************/

void SelectPipe::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void SelectPipe::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}


/***************Implementation for Project **************/

void Project::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void Project::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {

}



/***************Implementation for Join **************/

void Join::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void Join::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {

}


/***************Implementation for DuplicateRemoval **************/

void DuplicateRemoval::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void DuplicateRemoval::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {

}


/***************Implementation for Sum **************/

void Sum::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void Sum::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {

}


/***************Implementation for GroupBy**************/

void GroupBy::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void GroupBy::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {

}




/***************Implementation for WriteOut**************/

void WriteOut::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {


}

void WriteOut::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {

}
