#include "BigQ.h"
#include <pthread.h>
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include "Record.h"
#include "DBFile.h"
#include "HeapDBFile.h"

void *tpmms(void* arg) {
    BigQutil *bigQutil = (BigQutil *) arg;
    Record * temp = new Record();
    DBFile tempfile;
    tempfile.Create ("./bin/temp", heap, NULL);
    tempfile.Open("./bin/temp");


    int runlength = bigQutil->runlen;
    int run_size= PAGE_SIZE * runlength;
    //
    while(true) {
        vector<Record *> recArray;
        int totalsize = 0;
        int page_count =0;
        while(bigQutil->in->Remove(temp)) {
            //bigQutil->out->Insert(&temp);
            totalsize += temp->GetSize();

           // cout << totalsize << "   " <<  temp->GetSize () << endl;
            if(totalsize > PAGE_SIZE) {
                page_count++;
                totalsize = temp->GetSize();
            }
            if(page_count == runlength) {
                cout << "run completed" <<endl;
                break;
            }
            else {
                recArray.push_back(temp);
                temp = new Record();
            }
        }
        if(totalsize == 0) //no more records in input pipe
            break;
        sort(recArray.begin(),recArray.end(),RecordComparator(bigQutil->order));

        for (std::vector<Record *>::iterator it=recArray.begin(); it!=recArray.end(); ++it) {
            bigQutil->out->Insert((*it));
            tempfile.Add(*(*it));
            //vector <Record*> ::iterator ittem = recArray.erase(it);
            //delete (*ittem);
        }

    }

    // Imlementing the external sorting mechanism.

    // Read the data from Runs into the buffers.



    //free(bigQutil);
    tempfile.Close();
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	// Initialize Data Structures do I need it ?
	// Spawn a Worker thread
	/**
		Phase 1
		read run length no. of pages from InputPipe in Memory and append to DBFile
		Maintain run pointers for each run

		Phase 2
		Load r(no. of runs) buffers and have one output buffer
		extract min. among r buffers and write it to output buffer
		if the input buffer is empty load more from that run
		if the output buffer is full write it to disk
		if the run is completed scanning then mark it as inactive
		repeat the steps until all runs are scanned completely
	**/


	BigQutil bigQutil = {&in,&out, &sortorder, runlen};
	pthread_t worker_thread;
    pthread_create(&worker_thread,NULL,tpmms,(void *)&bigQutil);
    pthread_join(worker_thread,NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {
}
