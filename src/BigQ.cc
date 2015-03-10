#include "BigQ.h"


using namespace std;

void BigQ::mergeRuns()
{
	cout<<"Merging the Runs"<<endl;
	tempFile.Open(1,tempFileName);
   	priority_queue<RunRecord *, vector<RunRecord *>, CompareQueues> pQueue(&orderMaker);

	Page *inputPage  = new Page[runCounter];

	int whichPage[runCounter];

	vector<int> runStart;
	for(int i=0; i<runCounter; i++)
	{
        int count=0;
        for(vector<int>::iterator it=runL.begin(); it!=runL.begin()+i; it++)
        {
            count+=*it;
        }
        runStart.push_back(count);
	}


	//Initializing the first page of each run
	for(int i=0;i<runCounter;++i)
	{
        tempFile.GetPage(&(inputPage[i]),runStart[i]);
		whichPage[i]=0;
	}
	//Take the first record from eache page and initialize the queue.
	for(int i=0; i<runCounter; i++)
	{
	    RunRecord *runRecord=new RunRecord;
		runRecord->runNum=i;
		inputPage[i].GetFirst(&(runRecord->record));
		pQueue.push(runRecord);
	}

	while(true)
	{
		if(pQueue.empty())
			break;
		// Push the first record of the priority queue into the output pipe
		RunRecord *outputRecord = pQueue.top();
		pQueue.pop();
		int runNum = outputRecord->runNum;
		out->Insert(&(outputRecord->record));

		// Push the next record from the runs into the priority queue
		RunRecord *nextRunRecord=new RunRecord;

		// If the run page gets over...
		if(inputPage[runNum].GetFirst(&(nextRunRecord->record)) == 0)
		{
			whichPage[runNum]++;
			if(whichPage[runNum]>=runL[runNum])
				continue;
			tempFile.GetPage(&(inputPage[runNum]),whichPage[runNum]+runStart[runNum]);
			inputPage[runNum].GetFirst(&(nextRunRecord->record));
		}
		nextRunRecord->runNum=runNum;
		pQueue.push(nextRunRecord);
	}


    //finally shut down the out pipe
    tempFile.Close();
    delete [] inputPage;
}

void BigQ :: sortAndWriteToFile()
{
	if(myVector.empty())
		return;
	stable_sort(myVector.begin(),myVector.end(),CompareRecords(&orderMaker));
	int runLen=0;
	for(vector<Record *>::iterator myRecord=myVector.begin(); myRecord!=myVector.end(); myRecord++)
	{
		if(sortPage.Append(*myRecord))
			continue;

		writeToFile();
		sortPage.Append(*myRecord);
		runLen++;
	}

	writeToFile();
	runLen++;
	runCounter++;
	runL.push_back(runLen);
}

void BigQ :: writeToFile()
{
	off_t fileLength=tempFile.GetLength();
	off_t whichPage = (fileLength>0)?fileLength-1:0;

	tempFile.AddPage(&sortPage, whichPage);
    sortPage.EmptyItOut();
}

void BigQ :: freeResourcesPhaseOne()
{
	if(myVector.empty())
	{
		return;
	}
	for(vector<Record *>::iterator myRecord=myVector.begin(); myRecord!=myVector.end(); myRecord++)
    {
        delete *myRecord;
    }
    myVector.clear();
	tempFile.Close();
}

void BigQ::generateRuns()
{
	cout<<"Going into generating runs phase"<<endl;
	tempFile.Open(0,tempFileName);
	while(in->Remove(&myRecord))
	{
	    Record *cRecord=new Record;
	    cRecord->Copy(&myRecord);

	    if(myPage.Append(&myRecord))
	    {
	        myVector.push_back(cRecord);
	    }
	    else
	    {
	        pageCounter++;
	    	if(pageCounter==runLength)
	    	{
	        	sortAndWriteToFile();
				myVector.clear();
            	pageCounter=0;
	    	}

	        myPage.EmptyItOut();
            myPage.Append(&myRecord);
            myVector.push_back(cRecord);
	    }
	}

	myPage.EmptyItOut();
	sortPage.EmptyItOut();
	sortAndWriteToFile();
	cout<<"Number of runs : "<<runCounter<<endl;
	//free vector
    freeResourcesPhaseOne();
}

void* sortThread(void* arg)
{
    cout<<"Initializing the Run...."<<endl;
    BigQ *bigQ = new BigQ();
    bigQ = (BigQ*)arg;

    int num=rand()%10000;
	sprintf(bigQ->tempFileName,"%d.txt",num);

	bigQ->generateRuns();
	bigQ->mergeRuns();

    cout<<"Completed Merging of the Runs.."<<endl;
	bigQ->out->ShutDown ();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	this->in=&in;
	this->out=&out;
	this->orderMaker=sortorder;
	this->runLength=runlen;
    this->pageCounter = 0;
    this->runCounter = 0;
	pthread_t sortthread;
  	pthread_create(&sortthread,NULL,&sortThread,(void*)this);
}

BigQ::~BigQ () {
}

BigQ :: BigQ() {

}
