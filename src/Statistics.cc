#include "Statistics.h"
#include <stdio.h>
#include <float.h>
#include <set>
#include <cstring>
#include <iostream>

using namespace std;

Statistics::Statistics()
{

}
Statistics::Statistics(Statistics &copyMe)
{
    attInfoMap = copyMe.attInfoMap;
    relInfoMap = copyMe.relInfoMap;
}

Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    int groupId = relInfoMap.size() + 1;
    RelInfo *relInfo = new RelInfo(groupId,(double)numTuples);
    //updating blindly each time the client calls
    relInfoMap.insert(pair<char *,RelInfo *>(relName,relInfo));
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //RelInfo *relInfo = relInfoMap.find(relName)->second;
    AttInfo *attInfo = new AttInfo(relName,(double)numDistincts);
    attInfoMap.insert(pair<char *,AttInfo *>(attName,attInfo));
    //relInfo->addAttribute(attName,numDistincts);
   // relInfoMap.insert(pair<char *,RelInfo *>(relName,relInfo));
}

//TODO copy attributes also
void Statistics::CopyRel(char *oldName, char *newName)
{
    RelInfo *relInfo = relInfoMap.find(oldName)->second;
   // RelInfo *copyRelInfo = relInfo->copyMe();
   // copyRelInfo->updateRelName(newName);
   // relInfoMap.insert(pair<char *,RelInfo *>(newName,copyRelInfo));
    this->AddRel(newName,relInfo->numTuples);

    //One more better approach would be to calculate based on the run time
    //if the attName has dot instead of making duplicate keys
    map<char *,AttInfo *>::iterator attIte = attInfoMap.begin();
    for (attIte=attInfoMap.begin(); attIte!=attInfoMap.end(); ++attIte) {
        if(strcmp(oldName,attIte->second->relName) == 0) {
            AttInfo *attInfo = new AttInfo(newName,attIte->second->distTuples);
            string *temp = new string;
            temp->append(newName);
            temp->append(".");
            temp->append(attIte->first);
           // cout << (char *)temp->c_str() << endl;
            attInfoMap.insert(pair<char *,AttInfo *>((char *)temp->c_str(),attInfo));
        }
    }
}

void Statistics::Read(char *fromWhere)
{
}

void Statistics::Write(char *fromWhere)
{
    FILE *writeFile = fopen (fromWhere, "w");
    map<char *,RelInfo *>::iterator ite = relInfoMap.begin();
    fputs("[\n", writeFile);
    for (ite=relInfoMap.begin(); ite!=relInfoMap.end(); ++ite) {
        string temp = "";
        temp += ite->first;
        temp += "=";
        temp += ite->second->toString();
        temp += "|\n";
        fputs(temp.c_str(), writeFile);
    }
    fputs("]\n", writeFile);
    map<char *,AttInfo *>::iterator attIte = attInfoMap.begin();
    fputs("[\n", writeFile);
    for (attIte=attInfoMap.begin(); attIte!=attInfoMap.end(); ++attIte) {
        string temp = "";
        temp += attIte->first;
        temp += "=";
        temp += attIte->second->toString();
        temp += "|\n";
        fputs(temp.c_str(), writeFile);
    }
    fputs("]\n", writeFile);
    fclose(writeFile);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    set<char *> modifiedRel;
    double minimum = DBL_MAX;
   // cout << "array size " << sizeof(relNames)/sizeof(char *) << endl;
    bool isJoin = (numToJoin > 1 ? true : false);
    ParseAndList(parseTree, minimum, modifiedRel,isJoin);
   // cout << minimum << endl;
    if(isJoin) {
        set<char *>::iterator it;
        for (it=modifiedRel.begin(); it!=modifiedRel.end(); ++it)
        {
            RelInfo *relInfo = relInfoMap.find(*it)->second;
            relInfo->numTuples = minimum;
          //  cout << "min:;" << minimum << "relInfo" << relInfoMap.find(*it)->second->numTuples << endl;
        }
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    set<char *> modifiedRel;
    double minimum = DBL_MAX;
    map<char *, RelInfo *, cmp_str> relInfoMapTemp = relInfoMap;
    map<char *,RelInfo *>::iterator ite = relInfoMap.begin();
    bool isJoin = (numToJoin > 1 ? true : false);
    while(ite != relInfoMap.end())
    {
        relInfoMapTemp[ite->first] = (ite->second->copyMe());
        ++ite;
    }
    map<char *, AttInfo *, cmp_str> attInfoMapTemp = attInfoMap;
    map<char *,AttInfo *>::iterator attIte = attInfoMap.begin();
    while(attIte != attInfoMap.end())
    {
        attInfoMapTemp[attIte->first] = attIte->second->copyMe();
        ++attIte;
    }
    ParseAndList(parseTree, minimum, modifiedRel,isJoin);
    relInfoMap = relInfoMapTemp;
    attInfoMap = attInfoMapTemp;
    return minimum;
}

char * Statistics::ParseOperand(struct Operand *pOperand)
{
  //  cout << "ParseOperand" <<endl;
        if(pOperand!=NULL)
        {
                return pOperand->value;
        }
        return "";
}

double Statistics::ParseComparisonOp(struct ComparisonOp *pCom, set<char *>&  modifiedRel)
{
   // cout << "ParseComparisonOp"<<endl;
        if(pCom!=NULL)
        {

                char *left = ParseOperand(pCom->left);
               // cout << "ParseComparisonOp::" <<left <<endl;
                switch(pCom->code)
                {
                        case 1: case 2:
                            {
                                AttInfo *attInfo = attInfoMap.find(left)->second;
                                if(attInfo != NULL) {
                                    char *relName = attInfo->relName;
                                    RelInfo *relInfo = relInfoMap.find(relName)->second;
                                    modifiedRel.insert(relName);
                                 //   cout << "range" <<relInfo->numTuples/3.0 << endl;
                                    return (double)(relInfo->numTuples / 3.0);
                                }
                                else {
                                 //   cout << "Incorrect Parse tree specified no left operand" <<endl;
                                }
                                break;
                            }
                        case 3:
                            {
                                char *right = ParseOperand(pCom->right);
                                AttInfo *attInfoLeft = attInfoMap.find(left) == attInfoMap.end() ? NULL : attInfoMap.find(left)->second;
                                AttInfo *attInfoRight = attInfoMap.find(right) == attInfoMap.end() ? NULL :attInfoMap.find(right)->second;
                               // cout << "ParseComparisonOp::left" <<left  << endl;
                              // cout << "ParseComparisonOp::right" <<right  << endl;
                                if(attInfoLeft != NULL && attInfoRight != NULL) {
                                    char *relNameRight = attInfoRight->relName;
                                    RelInfo *relInfoRight = relInfoMap.find(relNameRight)->second;
                                    char *relNameLeft = attInfoLeft->relName;
                                    RelInfo *relInfoLeft = relInfoMap.find(relNameLeft)->second;
                                    modifiedRel.insert(relNameLeft);
                                    modifiedRel.insert(relNameRight);
                                    //cout << "given join query" <<relInfoRight->numTuples <<relInfoLeft->numTuples<<getMax(attInfoLeft->distTuples,attInfoRight->distTuples)<<endl;
                                    //merge groups
                                    map<char *,RelInfo *>::iterator ite = relInfoMap.begin();
                                    for (ite=relInfoMap.begin(); ite!=relInfoMap.end(); ++ite) {
                                        if(ite->second->groupId == relInfoRight->groupId) {
                                            ite->second->groupId = relInfoLeft->groupId;
                                        }
                                    }
                                   // cout<<relInfoRight->groupId << relInfoLeft->groupId <<endl;
                                  //  cout << relNameLeft << relNameRight <<endl;
                                    double distLeft = (double)getMin(relInfoLeft->numTuples,attInfoLeft->distTuples);
                                    double distRight = (double)getMin(relInfoRight->numTuples,attInfoRight->distTuples);
                            //        cout << distLeft << distRight << endl;
                             //       cout<< relInfoRight->numTuples << relInfoLeft->numTuples << endl;
                                    return (relInfoRight->numTuples * relInfoLeft->numTuples) / getMax(distLeft,distRight);
                                }
                                else if(attInfoLeft != NULL) {
                                 //   cout << "given point query" << endl;
                                    char *relNameLeft = attInfoLeft->relName;
                                    RelInfo *relInfoLeft = relInfoMap.find(relNameLeft)->second;
                                    modifiedRel.insert(relNameLeft);
                                    return (double)relInfoLeft->numTuples / (double)getMin(relInfoLeft->numTuples,attInfoLeft->distTuples);
                                }
                                else {
                                    cout << "Incorrect Parse tree specified no left/right operand for = operator" <<endl;
                                }
                                break;
                            }
                }
        }
        return -1;

}

double Statistics::ParseOrList(struct OrList *pOr,char *left,double maximum,set<char *>&  modifiedRel)
{
  //  cout << "ParseOrList"<<endl;
        if(pOr !=NULL)
        {
          //  cout<<"entered" << endl;
                struct ComparisonOp *pCom = pOr->left;
                Operand * leftOperand = pCom->left;
                if(leftOperand != NULL) {
                 //   cout<<"entered << oper" << endl;
                   if(left!=NULL && strcmp(left,ParseOperand(leftOperand)) == 0){
                           double temp= ParseComparisonOp(pCom, modifiedRel);
                       //    cout << "max" << maximum << temp <<endl;
                           maximum += temp;
                        //    cout<<"+OR" <<leftOperand->value <<endl;
                        //    cout << maximum << endl;
                   }
                   else {
                        double temp = ParseComparisonOp(pCom, modifiedRel);
                       //cout << "temp" << temp << endl;
                        if(temp > maximum)
                            maximum = temp;
                   }
                   left = ParseOperand(leftOperand);
                }
                if(pOr->rightOr)
                {
                      //  cout<<"entered right" << endl;
                        return ParseOrList(pOr->rightOr,left,maximum,modifiedRel);

                }
        }
        return maximum;
}

void Statistics::ParseAndList(struct AndList *pAnd,double& minimum,set<char *>&  modifiedRel,bool isJoin)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                minimum = ParseOrList(pOr,NULL,DBL_MIN,modifiedRel);
                /**if(temp < minimum) {
                    minimum = temp;
                }**/
               // cout << "Statistics::ParseAndList and expression" << minimum <<"temp"<<temp<< endl;
                //if(modifiedRel.size() >= 2) {
                    //cout << isJoin << endl;
                    if(isJoin) {
                            set<char *>::iterator it;
                            int groupId = -1;
                            for (it=modifiedRel.begin(); it!=modifiedRel.end(); ++it)
                            {
                                RelInfo *relInfo = relInfoMap.find(*it)->second;
                                groupId = relInfo->groupId;
                                break;
                               // cout << "min:;" << minimum << endl;
                            }
                            map<char *,RelInfo *>::iterator ite = relInfoMap.begin();
                            if(groupId != -1) {
                                while(ite != relInfoMap.end())
                                {
                                    if(ite->second->groupId == groupId) {
                                        ite->second->numTuples = minimum;
                                    }
                                    ++ite;
                                }
                            }
                    }

                //}
                if(pAnd->rightAnd)
                {
                        ParseAndList(pAnd->rightAnd, minimum, modifiedRel, isJoin);

                }
        }
        return;
}
