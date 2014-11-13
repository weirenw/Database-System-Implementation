#include "Statistics.h"

#define FOR_MAP(first,second,map)\
for(auto it = map.begin(); it != map.end(); it++){\
auto & first = it->first;\
auto & second = it->second;\

#define FOR_SET(first,set)\
for(auto it = set.begin(); it != set.end(); it++){\
auto & first = it->first;

#define END_FOR }

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    std::string rName(relName);
    relMap.erase(relMap.find(rName));
    rel r;
    r.numTul = numTuples;
    relMap.insert(std::pair<std::string, rel>(rName,r));
    set<string> relPar;
    relPar.insert(rName);
    relPartition.insert(pair<set<string>, double>(relPar,(double)numTuples));
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    std::string rName(relName);
    std::string aName(attName);
    rel r = relMap[rName];
    r.att.erase(r.att.find(aName));
    r.att.insert(std::pair<aName, numDistincts>);
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    std::string rOldName(oldName);
    std::string rNewName(newName);
    rel rOld = relMap[rOldName];
    rel rNew;
    
    //copy rOld data to rNew
    rNew.numTul = rOld.numTul;
    for (std::map<std::string, int>::iterator it = rOld.att.begin(); it != rOld.att.end(); it++) {
        rNew.att.insert(std::pair<it->first, it->second>);
    }
    //insert rNew, delete rOld?
    relMap.insert(std::pair<rNewName, rNew>);
    
    set<string> relNewPar;
    relNewPar.insert(rNewName);
    relPartition.insert(pair<set<string>, double>(relNewPar,(double)rNew.numTul));
}

void Statistics::Read(char *fromWhere)
{
    ifstream in;
    in.open(fromWhere);
    unsigned int relMapSize;
    in>>relMapSize;
    string rName;
    int rNumTul;
    string attName;
    int attNum;
    unsigned int rAttSize;
    while (relMapSize>0) {
        in>>rName>>rNumTul;
        AddRel(rName,rNumTul);
        in>>rAttSize;
        while (rAttSize>0) {
            in>>attName>>attNum;
            AddAtt(rName, attName, attNum);
            rAttSize--;
        }
        relMapSize--;
    }
    in.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream out;
    out.open(fromWhere);
    out<<(unsigned int)relMap.size()<<endl;
    FOR_MAP(rName, r, relMap)
    out<<rName<<endl;//relation name
    out<<r.numTul<<endl;//relation number tuples
    out<<(unsigned int)r.att.size()<<endl;//attribute size
    FOR_MAP(attName, attNum, r.att)
    out<<attName<<endl;
    out<<attNum<<endl;
    END_FOR
    END_FOR
    out.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    
}
bool Statistics::CheckRel(char **relNames, int numToJoin){
    set<string> relNameSet;
    for (int i=0; i<numToJoin; i++) {
        relNameSet.insert(relNames[i]);
    }
    
    FOR_SET(relNS, relNameSet)
    FOR_MAP(sRel, numTul, relPartition)
    if (sRel.find(relNs) != sRel.end()) {
        FOR_SET(relPS, sRel)
        if (strcmp(relNS, relPS) != 0) {
            return false;
        }
        END_FOR
        //check whether numTojoin == 0 in the end
        //exclude {A,B,E,X}
        numToJoin -= sRel.size();
    }
    END_FOR
    END_FOR
    
    return numToJoin==0;
}

