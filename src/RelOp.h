#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

	private:
	pthread_t thread;
	// Record *buffer;

    DBFile*    inputfile;
    Pipe*     outputpipe;
    CNF*       cnf;
    Record*   lit;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void PerformOperation ();
	void WaitUntilDone ();
	void Use_n_Pages (int n) {}

};

class SelectPipe : public RelationalOp {
    private:
	pthread_t thread;
	// Record *buffer;

    Pipe*    inputPipe;
    Pipe*     outputpipe;
    CNF*       cnf;
    Record*   lit;

	public:
	void PerformOperation ();
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n){}
};

class Project : public RelationalOp {
	pthread_t thread;
	// Record *buffer;

    Pipe*       inputPipe;
    Pipe*       outputPipe;
    int*        keep;
    int         numAttsInputs;
    int         numAttsoutputs;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void PerformOperation ();
	void WaitUntilDone ();
	void Use_n_Pages (int n){}
};
class Join : public RelationalOp {


    Pipe*       inPipeLeft;
    Pipe*       inPipeRight;
    Pipe*       outPutPipe;
    CNF*        cnf;
    Record*     lit;

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
    void PerformOperation ();
	void Use_n_Pages (int n);
};
class DuplicateRemoval : public RelationalOp {
	Pipe*       inputPipe;
    Pipe*       outPutPipe;
    Schema*   schema;
    int runlen;
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) ;
	void WaitUntilDone ();
	void PerformOperation ();
	void Use_n_Pages (int n){}
};
class Sum : public RelationalOp {

    Pipe*       inputPipe;
    Pipe*       outPutPipe;
    Function*   compute;
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
    void PerformOperation ();
	void Use_n_Pages (int n) {}
};
class GroupBy : public RelationalOp {

    Pipe*       inputPipe;
    Pipe*       outPutPipe;
    Function*   compute;
	pthread_t thread;
    OrderMaker* groupAttrs;
    int         runLen;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
    void PerformOperation ();
	void Use_n_Pages (int n) { runLen = n;}
};
class WriteOut : public RelationalOp {
	Pipe* inputPipe;
	FILE* outputFile;
	Schema* schema;
	pthread_t thread;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void PerformOperation ();
	void WaitUntilDone ();
	void Use_n_Pages (int n) {}
};
#endif
