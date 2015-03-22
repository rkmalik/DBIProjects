#include "RelOp.h"

struct SelectMethodArgs {

    DBFile &    inputfile;
    Pipe &      outpipe;
    CNF&        cnf;
    Record &    lit;

};

void* SelectFromFileMethod (void * args)
{
    SelectFile * selectfile = (SelectFile*) args;
    selectfile->PerformOperation();
}

void SelectFile::PerformOperation ()
{
    Record outputrecord;

    while (inputfile->GetNext(outputrecord, *cnf, *lit)) {

        cout << "Setting up record in output pipe";
        outputpipe->Insert(&outputrecord);
    }
    cout << "Shutting down the pipe" << endl;
    outputpipe->ShutDown();
}
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

    /*struct SelectMethodArgs myarg = {inFile, outPipe, selOp, literal};*/
    inputfile = &inFile;
    outputpipe = &outPipe;
    cnf = &selOp;
    lit = &literal;
    pthread_create(&thread,NULL,SelectFromFileMethod,(void*)this);
}



void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{

}
void SelectPipe::WaitUntilDone () {

}
void SelectPipe::Use_n_Pages (int n) {

}
