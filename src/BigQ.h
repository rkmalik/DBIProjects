#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"


using namespace std;

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
     return ceng.Compare(x,y,this->order) > 0 ? true : false;
   }
};

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
