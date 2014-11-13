//
//  Op.h
//  DBI_Weiren
//
//  Created by Weiren Wang on 3/17/14.
//  Copyright (c) 2014 DBI_Weiren. All rights reserved.
//
#include<pthread.h>
#include<unistd.h>

#include "BigQ.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Function.h"
#include "Pthreadutil.h"   // pack, unpact, etc
#include "Defs.h"
#ifndef DBI_Weiren_Op_h
#define DBI_Weiren_Op_h
class JoinBuffer;
class RelationalOp {
public:
    // blocks the caller until the particular relational operator // has run to completion
    virtual void WaitUntilDone ();
    // tells how much internal memory the operation can use
    virtual void Use_n_Pages (int n) = 0;
    
protected:
    pthread_t worker;
    
};

class SelectPipe: public RelationalOp{
    
public:
    
    void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void Use_n_Pages(int n){};
    
private:
    MAKE_STRUCT4(Args, Pipe*, Pipe*, CNF*, Record*);
    static void* work(void *param);
};

class SelectFile: public RelationalOp{
    
public:
    
    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void Use_n_Pages(int n){};
    
private:
    MAKE_STRUCT4(Args, DBFile*, Pipe*, CNF*, Record*);
    static void* work(void *param);
    
};

class Project: public RelationalOp{
public:
    
    void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void Use_n_Pages(int n){};
private:
    
    MAKE_STRUCT5(Args, Pipe*, Pipe*, int*, int, int);
    static void* work(void *param);
    
};

class JoinBuffer;
class Join : public RelationalOp {
    
public:
    void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF&selOp, Record &literal);
    void Use_n_Pages (int n) {runLen = n;}
    Join(): runLen(128) {}
    size_t runLen;
    
private:
    MAKE_STRUCT6(Args, Pipe*, Pipe* , Pipe*, CNF*, Record*, size_t);
    static void* work(void* param);
    static void sortMergeJoin(Pipe *inL, Pipe *inR, Pipe *out, CNF *cnf, Record *rec, size_t runLen, OrderMaker *omL, OrderMaker *omR);
    static void nestedLoopJoin(Pipe *inL, Pipe *inR, Pipe *out, CNF *cnf, Record *rec, size_t runLen);
    static void joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& sleOp);
    static void dumpFile(Pipe &in, DBFile &out);
};

class JoinBuffer{
    friend class Join;
    JoinBuffer(size_t npages);
    ~JoinBuffer();

    bool add(Record &addMe);
    void clear(){
        size = nrecords=0;
    };
    
    size_t size, capacity;
    size_t nrecords;
    Record *buffer;
};

class DuplicateRemoval : public RelationalOp {
public:
    
    void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void Use_n_Pages (int n) {runLen = n;}
    DuplicateRemoval(): runLen(128) {}
    size_t runLen;
private:
    MAKE_STRUCT4(Args, Pipe*, Pipe*, Schema*, size_t);
    static void* work(void* param);
};

class Sum: public RelationalOp {
public:
        void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    	void Use_n_Pages(int n){};
private:
    MAKE_STRUCT3(Args, Pipe*, Pipe*, Function *);
    static void* work(void* param);
};

class GroupBy : public RelationalOp {
public:
    void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void Use_n_Pages (int n) {runLen = n;}
    GroupBy(): runLen(128) {}
    size_t runLen;
private:
    MAKE_STRUCT5(Args, Pipe*, Pipe*, OrderMaker*, Function*, size_t);
    static void* work(void *param);
    static void putGroup(Record& rec,  const int& sumint, const double& sumdouble, Pipe* out,  OrderMaker* om){
        rec.Project(om->getAtts(), om->getNumAtts(), rec.numAtts());
	if(sumint != 0){
        	rec.prepend(sumint);
        }
	else if(sumdouble != 0){
		rec.prepend(sumdouble);
	}
	out->Insert(&rec);
    }
};

class WriteOut : public RelationalOp {
public:
    void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void Use_n_Pages(int n){};
private:
    MAKE_STRUCT3(Args, Pipe*, FILE*, Schema*);
    static void* work(void *param);
};


#endif
