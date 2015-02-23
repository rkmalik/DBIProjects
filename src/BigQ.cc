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

    // This holds the Page number of each run head
    vector <int> runhead;
    int pageNum = 0;
    //
    while(true) {
        vector<Record *> recArray;
        int totalsize = 0;
        int page_count =0;
        runhead.push_back(pageNum);
        while(bigQutil->in->Remove(temp)) {
            //bigQutil->out->Insert(&temp);
            totalsize += temp->GetSize();

           // cout << totalsize << "   " <<  temp->GetSize () << endl;
            if(totalsize > PAGE_SIZE) {
                page_count++;
                pageNum ++;
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

    /*
        Read the data from the file and put the records in the Priority Queue
        Based on the comparison criteria. Get the maximum item from priority qu5/eue.
        and put that in the external pipe. At the same time put the data in teh saame run
        from which data is just removed.

        Run head is holding the top page number of each head.
    */

    priority_queue <pair<int, Record*>, vector <pair <int, Record*> >, PairSorter> sortq (PairSorter (*(bigQutil->order)));
    vector <int> runcheck (runhead);
    vector <Page *> runpagelist;
    // Now I will initialize the data in this priority queue.
    for (int i = 0; i < runhead.size () -1; i++)
    {
        Page * mypage = new Page ();
        tempfile.GetPage(mypage, runhead[i]);
        // Load the page from the file
        Record * myrec = new Record ();
        mypage->GetFirst(myrec);
        pair<int, Record *> * mypair = new pair<int, Record*>  (i, myrec);
        sortq.push (*mypair);
        runpagelist.push_back (mypage);
    }

    // Till now I have taken records from each run and put it into the priority queue.
    // Now i need to pick the first record from the top and put that into the output pipe.

    while (!sortq.empty ())
    {
        pair<int, Record*>  mypairtemp = (sortq.top ());
        int runnum = mypairtemp.first;
        Record * myrec = mypairtemp.second;
        bigQutil->out->Insert(myrec);
        sortq.pop ();


        // Now take the record from the RunNumber +1; and place that in the priority queue
        if(!runpagelist[runnum]->GetFirst(myrec)) {
            // Check if the current Offset is less than the next run start offset then it is fine to take the new record
            if (++runcheck[runnum]< runhead[runnum+1]) {

                Record * myrec = new Record ();
                Page * mypage = new Page ();
                tempfile.GetPage(mypage, runhead[runnum]);
                mypage->GetFirst(myrec);
                pair<int, Record *> * mypair = new pair<int, Record*>  (runnum, myrec);
                sortq.push (*mypair);
                runpagelist[runnum] = mypage;
            }

        } else {
            /*Record * myrec = new Record ();
            Page * mypage = new Page ();
            tempfile.GetPage(mypage, runhead[i]);
            mypage.GetFirst(myrec);*/
            pair<int, Record *> * mypair = new pair<int, Record*>  (runnum, myrec);
            sortq.push (*mypair);
        }

    }

    for(int i=0; i<runpagelist.size(); i++){
		delete runpagelist[i];
	}


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
