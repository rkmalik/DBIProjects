#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include "string.h"

#include <map>
#include <string>
#include <sstream>
#include <set>

using namespace std;

class RelInfo {
    public:
    double numTuples;
    int groupId;
    //char * relName;
    //map<char *,int> attNameMap;
    RelInfo(int groupId, double numTuples) {
        this->groupId = groupId;
        this->numTuples = numTuples;
    }

    void updateTupleCount(double numTuples) {
        this->numTuples = numTuples;
    }

    /**void updateRelName(char * relName) {
        this->relName = relName;
    }**/

    /**void addAttribute(char * attName,int numDistincts) {
        attNameMap.insert(pair<char *,int>(attName,numDistincts));
    }**/

    RelInfo * copyMe() {
        RelInfo* relInfo = new RelInfo(this->groupId,this->numTuples);
       // relInfo->attNameMap = this->attNameMap; // = operator is overloaded in map and does deep copy
        return relInfo;
    }

    string toString() {
        string temp = "";
        std::ostringstream ss;
        ss << numTuples;
		temp = temp + ss.str();
		ss.clear();
		ss.str("");
        temp += ":";
       /** temp += relName;
        temp += ":";**/
        ss << groupId;
		temp = temp + ss.str();
        /**temp += "[";
        map<char *,int>::iterator ite = attNameMap.begin();
        for (ite=attNameMap.begin(); ite!=attNameMap.end(); ++ite) {
                temp += ite->first;
                temp += ",";
                ss << ite->second;
                temp += ss.str();
                temp += ";";
        }
        temp += "]";**/
        return temp;
    }
};

class AttInfo {
    public:
    char * relName;
    double    distTuples;
    AttInfo(char *relName,double distTuples) {
        this->relName = relName;
        this->distTuples = distTuples;
    }

    string toString() {
        string temp = "";
        std::ostringstream ss;
        temp += relName;
        temp += ":";
        ss << distTuples;
		temp = temp + ss.str();
        return temp;
    }

    AttInfo * copyMe() {
        AttInfo* attInfo = new AttInfo(this->relName,this->distTuples);
       // relInfo->attNameMap = this->attNameMap; // = operator is overloaded in map and does deep copy
        return attInfo;
    }
};

struct cmp_str
{
   bool operator()(char const *a, char const *b)
   {
      return strcmp(a, b) < 0;
   }
};

class Statistics
{

public:
    //make them private later on
    map<char *, RelInfo *, cmp_str> relInfoMap;
    map<char *, AttInfo *, cmp_str> attInfoMap;
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	double getMax(double x,double y) {
        return x > y ? x : y;
    }

    double getMin(double x,double y) {
        return x < y ? x : y;
    }
    char * ParseOperand(struct Operand *pOperand);
    double ParseComparisonOp(struct ComparisonOp *pCom, set<char *>&  modifiedRel);
    double ParseOrList(struct OrList *pOr,char *left,double maximum,set<char *>&  modifiedRel);
    void ParseAndList(struct AndList *pAnd,double& minimum,set<char *>&  modifiedRel,bool isJoin);

};

#endif
