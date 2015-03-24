#include "RelOp.h"

void* SelectFromFileMethod (void * args)
{
    SelectFile * selectfile = (SelectFile*) args;
    selectfile->PerformOperation();
}

void SelectFile::PerformOperation ()
{
    Record outputrecord;
    int i=0;
    while (inputfile->GetNext(outputrecord, *cnf, *lit)) {

        //cout << "Setting up record in output pipe";
        i++;
        outputpipe->Insert(&outputrecord);
    }
   // cout<<"scanned" << i << " records";
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

void * duplicateRemovalMethod(void* args) {
    DuplicateRemoval * duplicateremoval = (DuplicateRemoval*) args;
    duplicateremoval->PerformOperation();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    inputPipe = &inPipe;
    outPutPipe = &outPipe;
    schema = &mySchema;
    pthread_create(&thread,NULL,duplicateRemovalMethod,(void *)this);

}

void DuplicateRemoval::WaitUntilDone () {
    pthread_join (thread, NULL);
}


void DuplicateRemoval::PerformOperation () {

    int  bufSize = 100;
    OrderMaker* ordermaker = new OrderMaker(schema);
    Pipe* intermediatepipe  = new Pipe (bufSize);
    runlen = 20;
    BigQ* bq = new BigQ (*inputPipe, *intermediatepipe, *ordermaker, runlen);

	Record* outputrecord = new Record;
	Record prev;
	int i=0;
    if(intermediatepipe->Remove(outputrecord)) {
        prev.Copy(outputrecord);
        outPutPipe->Insert(outputrecord);
        delete outputrecord;
		outputrecord = new Record;
		i++;
	}
	ComparisonEngine ceng;

	while (intermediatepipe->Remove (outputrecord)) {
        if (ceng.Compare (&prev, outputrecord, ordermaker) != 0) {
                prev.Copy(outputrecord);
                outPutPipe->Insert(outputrecord);
                i++;
        }

		delete outputrecord;
		outputrecord = new Record;
	}
   // cout<<"unique" << i<<endl;
    inputPipe->ShutDown();
    intermediatepipe->ShutDown();
    outPutPipe->ShutDown();
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
    int i=0;
    while (inputPipe->Remove(outputrecord)) {
     //   cout<<"hello";
        //outputrecord->Print(schema);
        i++;
        fputs(outputrecord->PrintRecord(schema), outputFile);
        outputrecord = new Record;
    }
  //  cout<<"records written" << i<<endl;
    inputPipe->ShutDown();
}

void WriteOut::WaitUntilDone () {
     pthread_join (thread, NULL);
}

void *SumMethod(void* args)
{
    Sum * sumobj = (Sum*) args;
    cout << "Sum"<< endl;
    sumobj->PerformOperation();
}


void Sum::PerformOperation ()
{
    Record* outputrecord = new Record ();
    Record* buffer = new Record ();
    double doublesum = 0;
    int     intsum =0;
    double  doubleresult =0;
    int     intresult =0;
    Type    rectype = compute->returnsInt? Int : Double;

    while (inputPipe->Remove(outputrecord)) {

        doubleresult =0;
        intresult =0;

        compute->Apply(*outputrecord, intresult, doubleresult);

        if (compute->returnsInt) {
            intsum = intsum + intresult;
        } else {
            doublesum = doublesum + doubleresult;
        }
    }


    //delete outputrecord;
    buffer->CreateRecord (rectype, intsum, doublesum);
    outPutPipe->Insert(buffer);
    // Create a new record and put in the output pipe
    inputPipe->ShutDown();
    outPutPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
    inputPipe = &inPipe;
    outPutPipe = &outPipe;
    compute = &computeMe;
        cout << "Sum"<< endl;
    pthread_create(&thread,NULL,SumMethod,(void *)this);
}

void Sum::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}

void *GroupByMethod(void* args)
{
    GroupBy * groupByObj = (GroupBy*) args;

    groupByObj->PerformOperation();
}

void GroupBy::PerformOperation ()
{
    // I will create a BigQ to read the data from the input pipe and sort this
    // based on the order Marker
    int                 bufSize = 100;
    Type                recType;
    ComparisonEngine    ceng;
    int                 intResult;
    int                 intAttVal;
    double              doubleResult;
    double              doubleAttVal;
    int                 attCount = groupAttrs->numAtts;
    int                 attKeep [attCount+1];


    Record* sumRecord   = new Record;
    Record* recLeft     = new Record;
    Record* recRight    = new Record;
    Record* tempRecord1 = new Record ();
    Record* last        = NULL;
    Record* prev        = NULL;
    Record  rec[2];

    cout << "Calliing Group By" << endl;


    //int                 recSwitch = 0;
    bool                recSwitch = false;

    Pipe* intermediatepipe  = new Pipe (bufSize);
    BigQ* bq                = new BigQ (*inputPipe, *intermediatepipe, *groupAttrs, runLen);

    for (int i = 0; i <= attCount; i++) {
        attKeep [i] = groupAttrs->whichAtts[i-1];
    }


    if (compute->returnsInt) {
        recType = Int;
    } else {
        recType = Double;
    }


    int inserted = 0;
    int counter = 0;

    // Loop till all the records from intermediate pipe are consumed
    while (intermediatepipe->Remove(&rec [counter%2])){
        prev = last;
        last = &rec[counter%2];

        if (prev && last) {
            if (ceng.Compare (prev, last, groupAttrs) !=0 ) {
                compute->Apply(*prev,intAttVal,doubleAttVal);

                if (compute->returnsInt) {
                    intResult = intResult + intAttVal;
                } else {
                    doubleResult = doubleResult + doubleAttVal;
                }

                recLeft->CreateRecord(recType,intResult,doubleResult);
                sumRecord->MergeRecords(recLeft,prev,1 ,((int *) prev->bits)[1] / sizeof(int) -1, attKeep, (groupAttrs->numAtts)+1,1);
                outPutPipe->Insert(sumRecord);
                intResult = 0;
                doubleResult = 0;
                inserted++;
            } else {

                compute->Apply(*prev,intAttVal,doubleAttVal);

                if (compute->returnsInt) {
                    intResult = intResult + intAttVal;
                } else {
                    doubleResult = doubleResult + doubleAttVal;
                }
            }
        }
        counter++;
    }

    compute->Apply(*last,intAttVal,doubleAttVal);

    if (compute->returnsInt) {
        intResult = intResult + intAttVal;
    } else {
        doubleResult = doubleResult + doubleAttVal;
    }

    recLeft->CreateRecord(recType,intResult,doubleResult);
    sumRecord->MergeRecords(recLeft,last,1 ,((int *) last->bits)[1] / sizeof(int) -1, attKeep, (groupAttrs->numAtts)+1,1);
    outPutPipe->Insert(sumRecord);
    inserted++;
    intermediatepipe->ShutDown();
    //inputPipe->ShutDown();
    //outPutPipe->ShutDown();
    cout << "Total Records Scanned in GroupBy : " << inserted << endl;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
    inputPipe = &inPipe;
    outPutPipe = &outPipe;
    compute = &computeMe;
    groupAttrs = &groupAtts;
    pthread_create(&thread,NULL,GroupByMethod,(void *)this);
}

void GroupBy::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}

void *JoinMethod(void* args) {
    Join *join = (Join *)args;
    join->PerformOperation();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
    inPipeLeft = &inPipeL;
    inPipeRight = &inPipeR;
    outPutPipe = &outPipe;
    cnf = &selOp;
    lit  = &literal;
    pthread_create(&thread,NULL,JoinMethod,(void *)this);
}

void Join::PerformOperation()
{
    OrderMaker orderMakerL;
    OrderMaker orderMakerR;
    Record * left=new Record;
    Record * right=new Record;
    Record *buffer = new Record;
    ComparisonEngine ceng;
    Pipe *outPipeLeft=new Pipe(100);
    Pipe *outPipeRight=new Pipe(100);
    vector<Record*> blockL;
    vector<Record*> blockR;
    Record *result=new Record;
    int numOfAttsL;
    int numOfAttsR;
    int runlen = 12;
    int record_count =0 ;
    if((cnf)->GetSortOrders(orderMakerL,orderMakerR))
    {
        BigQ bqL (*inPipeLeft, *outPipeLeft, orderMakerL, runlen);
        BigQ bqR (*inPipeRight, *outPipeRight, orderMakerR, runlen);
        cout << "BigQ called"<<endl;
        outPipeLeft->Remove(left);
        outPipeRight->Remove(right);
        bool leftEnd=false;
        bool rightEnd=false;
        numOfAttsL=((int *) left->bits)[1] / sizeof(int) -1;
        numOfAttsR=((int *) right->bits)[1] / sizeof(int) -1;
        int attsToKeep[numOfAttsL+numOfAttsR];
        for(int i=0; i<numOfAttsL; i++)
        {
            attsToKeep[i]=i;
        }
        for(int i=0; i<numOfAttsR; i++)
        {
            attsToKeep[numOfAttsL+i]=i;
        }
       // cout<< "nums" << numOfAttsL <<endl;
        while (true)
        {
            if(ceng.Compare(left,&orderMakerL,right,&orderMakerR)<0)
            {
                if(!outPipeLeft->Remove(left))
                    break;
            }
            else if(ceng.Compare(left,&orderMakerL,right,&orderMakerR)>0)
            {
                if(!outPipeRight->Remove(right))
                    break;
            }
            else
            {
                leftEnd=false;
                rightEnd=false;
                while(true)
                {
                    Record *prev=new Record;
                    prev->Consume(left);
                    blockL.push_back(prev);
                    if(!(outPipeLeft->Remove (left)))
                    {
                        leftEnd=true;
                        break;
                    }
                    if(ceng.Compare (prev, left,&orderMakerL) !=0)
                    {
                        break;
                    }
                }
                while(true)
                {
                    Record *prev=new Record;
                    prev->Consume(right);
                    blockR.push_back(prev);
                    if(!(outPipeRight->Remove (right)))
                    {
                        rightEnd=true;
                        break;
                    }
                    if(ceng.Compare (prev,right,&orderMakerR) !=0)
                    {
                        break;
                    }
                }
                for (int i = 0; i < blockL.size(); i++)
                {
                    for (int j = 0; j < blockR.size(); j++)
                    {
                        result->MergeRecords(blockL.at(i),blockR.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
                        //cout<<"inserted a record"<<endl;
                        record_count++;
                        (outPutPipe)->Insert(result);
                    }
                    delete blockL.at(i);
                }
                for (int j = 0; j < blockR.size(); j++)
                {
                    delete blockR.at(j);
                }
                blockL.clear();
                blockR.clear();
                cout<< rightEnd << leftEnd << record_count << endl;
                if(rightEnd || leftEnd)
                    break;
            }
        }
        cout<<"out of while"<<endl;
        while(outPipeLeft->Remove(left))
        {}
        while(outPipeRight->Remove(left))
        {}
    }
    else
    {
       // cout << "else part" <<endl;
        while((inPipeRight)->Remove(right))
        {
            Record *temp=new Record;
            temp->Consume(right);
            blockR.push_back(temp);
        }
        (inPipeLeft)->Remove(left);
        numOfAttsL=((int *) left->bits)[1] / sizeof(int) -1;
        numOfAttsR=((int *) blockR.at(0)->bits)[1] / sizeof(int) -1;
        cerr<<"numOfAttsL "<<numOfAttsL<<"numOfAttsR "<<numOfAttsR<<endl;
        int attsToKeep[numOfAttsL+numOfAttsR];
        for(int i=0; i<numOfAttsL; i++)
        {
            attsToKeep[i]=i;
        }
        for(int i=0; i<numOfAttsR; i++)
        {
            attsToKeep[numOfAttsL+i]=i;
        }
        int count=1;
        do
        {
            for (int j = 0; j < blockR.size(); j++)
            {
                result->MergeRecords(left,blockR.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
                (outPutPipe)->Insert(result);
            }
            cerr<<count++<<endl;
        }
        while((inPipeLeft)->Remove(left));
    }
    (outPutPipe)->ShutDown();
    delete outPipeLeft;
    delete outPipeRight;
    delete result;
    cout<<"Join Completed" << endl;
}

void Join::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
