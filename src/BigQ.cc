#include "BigQ.h"
#include <pthread.h>
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>


void *tpmms(void* arg) {
    BigQutil *bigQutil = (BigQutil *) arg;
    Record temp;
    vector<Record*> recArray;
    while(bigQutil->in->Remove(&temp)) {
        //bigQutil->out->Insert(&temp);
        recArray.push_back(&temp);
    }
    sort(recArray.begin(),recArray.end(),RecordComparator(bigQutil->order));
    for (std::vector<Record*>::iterator it=recArray.begin(); it!=recArray.end(); ++it)
        bigQutil->out->Insert(*it);

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
