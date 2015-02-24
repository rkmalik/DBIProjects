#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <queue>


using namespace std;

class RecordSorter {

    OrderMaker & _sortorder;

public:
    RecordSorter (OrderMaker & sortorder):_sortorder(sortorder) { }

    bool operator () (Record * r1, Record * r2) {
        ComparisonEngine compengine;

        if (compengine.Compare(r1, r2, &_sortorder) < 0)
            return true;
        else
            return false;
    }
};

class PairSorter {

    OrderMaker & _sortorder;

public :
    PairSorter(OrderMaker & sortorder):_sortorder(sortorder){}

    bool operator () (pair <int, Record *> r1, pair <int, Record *> r2)
    {
        ComparisonEngine compengine;

        if (compengine.Compare(r1.second, r2.second, &_sortorder) < 0)
            return false;
        else
            return true;

    }
};

typedef struct {
        Pipe *in;
        Pipe *out;
	    OrderMaker *order;
        int runlen;
} BigQutil;

struct  RecordComparator {
   OrderMaker *order;
   RecordComparator(OrderMaker* order) {
        this->order = order;
   }

   bool operator() (Record* x, Record* y) {
     ComparisonEngine ceng;
     return ceng.Compare(x,y,this->order) < 0 ? true : false;
   }
};

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
