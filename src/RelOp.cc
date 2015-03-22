#include "RelOp.h"

void* SelectFromFileMethod (void * args)
{
    SelectFile * selectfile = (SelectFile*) args;
    selectfile->PerformOperation();
}

void SelectFile::PerformOperation ()
{
    Record outputrecord;

    while (inputfile->GetNext(outputrecord, *cnf, *lit)) {

        //cout << "Setting up record in output pipe";
        outputpipe->Insert(&outputrecord);
    }
    //cout << "Shutting down the pipe" << endl;
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

void* SelectFromPipeMethod (void * args)
{
    SelectPipe * selectpipe = (SelectPipe*) args;
    selectpipe->PerformOperation();
}

void SelectPipe::PerformOperation ()
{
    Record outputrecord;
    ComparisonEngine e;
    while (inputPipe->Remove(&outputrecord)) {
        if (e.Compare(&outputrecord, lit, cnf) == 1){
            outputpipe->Insert(&outputrecord);
        }
    }
    outputpipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
    inputPipe = &inPipe;
    outputpipe = &outPipe;
    cnf = &selOp;
    lit = &literal;
    pthread_create(&thread,NULL,SelectFromPipeMethod,(void*)this);

}

void SelectPipe::WaitUntilDone () {
    pthread_join(thread, NULL);

}

void SelectPipe::Use_n_Pages (int runlen) {

}


void * DisplayInOrderOp (void * args)
{
    Project * project = (Project*) args;
    project->PerformOperation();
}

void Project::PerformOperation ()
{
    Record* outputrecord = new Record;
    while (inputPipe->Remove(outputrecord)) {
        //cout << outputrecord<< endl;
        outputrecord->Project(keep, numAttsoutputs, numAttsInputs);
        outputPipe->Insert(outputrecord);
        outputrecord = new Record;
    }
    inputPipe->ShutDown();
    outputPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    keep = keepMe;
    numAttsInputs = numAttsInput;
    numAttsoutputs = numAttsOutput;
  //  cout << "Calling Project" << endl;
    pthread_create(&thread,NULL,DisplayInOrderOp,(void*)this);

}
void Project::WaitUntilDone ()
{
    pthread_join (thread, NULL);

}
void Project::Use_n_Pages (int n)
{


}

void *writeOutMethod(void* args) {
    WriteOut * writeout = (WriteOut*) args;
    writeout->PerformOperation();
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    inputPipe = &inPipe;
    outputFile = outFile;
    schema = &mySchema;
    pthread_create(&thread,NULL,writeOutMethod,(void *)this);

}

void WriteOut::PerformOperation () {
    Record* outputrecord = new Record;
   //cout<<"perForm"<<endl;
    while (inputPipe->Remove(outputrecord)) {
     //   cout<<"hello";
        outputrecord->Print(schema);
        fputs(outputrecord->PrintRecord(schema), outputFile);
        outputrecord = new Record;
    }
    inputPipe->ShutDown();
}

void WriteOut::WaitUntilDone () {
     pthread_join (thread, NULL);
}
