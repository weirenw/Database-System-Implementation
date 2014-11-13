//
//  Op.cc
//  DBI_Weiren
//
//  Created by Weiren Wang on 3/17/14.
//  Copyright (c) 2014 DBI_Weiren. All rights reserved.
//

#include <stdio.h>
#include <iostream>
#include <cstdlib>

#include "RelOp.h"
#include "Record.h"
#include "Errors.h"
#include "HeapFile.h"

#define FOR_INPIPE(rec,in) \
 Record rec;                \
 while(in->Remove(&rec)){ 

#define FOR_BUFFER(ele, buffer, bufsize) \
    for(typeof(buffer) p = buffer; p!=buffer+(bufsize); p++){ \
          typeof(*p) & ele = *p; 

#define FOREACH_INFILE(rec, f)                  \
f.MoveFirst();                                \
Record rec;                                   \
while (f.GetNext(rec))

#ifndef END_FOR
#define END_FOR }
#endif

class JoinBuffer;
void Join::nestedLoopJoin(Pipe* inL, Pipe* inR, Pipe* out, CNF* selOp, Record* literal, size_t runLen){
    DBFile rightFile;
    dumpFile(*inR, rightFile);
    JoinBuffer leftBuffer(runLen);
//	cout<<"nestedLoopJoin begin"<<endl;    
    FOR_INPIPE(rec, inL)
    if (!leftBuffer.add(rec)) {
        joinBuf(leftBuffer, rightFile, *out, *literal, *selOp);
        leftBuffer.clear();
        leftBuffer.add(rec);
    }
    END_FOR
    joinBuf(leftBuffer, rightFile, *out, *literal, *selOp);
    rightFile.Close();
    
//	cout<<"nestedLoopJoin end"<<endl;    
}
void Join::joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
    ComparisonEngine cmp;
    Record merged;
//    cout<<"joinBuf begin"<<endl; 
    FOREACH_INFILE(fromFile, file) {
        FOREACH(fromBuffer, buffer.buffer, buffer.nrecords)
        if (cmp.Compare(&fromBuffer, &fromFile, &literal, &selOp)) {   // actural join
            merged.CrossProduct(&fromBuffer, &fromFile);
            out.Insert(&merged);
        }
        END_FOR
    }
//    cout<<"joinBuf end"<<endl;
}

void Join::dumpFile(Pipe& in, DBFile& out) {
    const int RLEN = 10;
    char rstr[RLEN];
//	cout<<"dumpFile begin"<<endl;
    Rstring::gen(rstr, RLEN);  // need a random name otherwise two or more joins would crash
    std::string tmpName("join");
    tmpName = tmpName + rstr + ".tmp";
    out.Create((char*)tmpName.c_str(), heap, NULL);
    Record rec;
    while (in.Remove(&rec)) out.Add(rec);
	
//	cout<<"dumpFile end"<<endl;
}

void Join::sortMergeJoin(Pipe *inL, Pipe *inR, Pipe *out, CNF *cnf, Record *literal, size_t runLen, OrderMaker *omL, OrderMaker* omR){
    
//	cout<<"sortMergeJoin begin"<<endl;
    Pipe sortL(PIPE_SIZE);
    Pipe sortR(PIPE_SIZE);
    BigQ bqL(*inL,sortL,*omL,runLen);
    BigQ bqR(*inR,sortR,*omR,runLen);
    ComparisonEngine cmp;
    JoinBuffer jBuff(runLen);
    
    Record recL, recR, recP, merged;//left  record, right record, previous record
    bool bRecL, bRecR;
    for (bRecL = sortL.Remove(&recL), bRecR = sortR.Remove(&recR); bRecL && bRecR; ) {
        int result = cmp.Compare(&recL, omL, &recR, omR);
        if (result < 0) {//left is less
            bRecL = sortL.Remove(&recL);
        }
        else if(result > 0){//left is more
            bRecR = sortR.Remove(&recR);
        }
        else{//left right equal
            //cout<<"left right equal begin"<<endl;
            jBuff.clear();
	    for(recP.Consume(&recL);(bRecL=sortL.Remove(&recL)) && cmp.Compare(&recP, &recL,omL)==0;recP.Consume(&recL))
	jBuff.add(recP);
	jBuff.add(recP);


            
            do{
                FOR_BUFFER(rec, jBuff.buffer, jBuff.nrecords)
                if(cmp.Compare(&rec, &recR, literal, cnf)){
                    merged.CrossProduct(&rec, &recR);
                    out->Insert(&merged);
                }
                END_FOR
            }while ((bRecR=sortR.Remove(&recR))&&(cmp.Compare(jBuff.buffer, omL, &recR, omR))==0);
	//cout<<"left right equal end"<<endl;
	}
    }
	
//	cout<<"sortMergeJoin end"<<endl;
	//usleep(10000000);
        //cout<<"wake up"<<endl;
}

void* Join::work(void *param){
    UNPACK_ARGS6(Args, param, inL , inR , out , sel , rec , runLen);
    OrderMaker omleft, omright;
    if(sel->GetSortOrders(omleft,omright)){
        //sort merge join
//     	cout<<"Join work sortmerge begin"<<endl;
        sortMergeJoin(inL,inR,out,sel,rec,runLen,&omleft,&omright);
//     	cout<<"Join work sortmerge end"<<endl;
    }
    else{
//     	cout<<"Join work nested begin"<<endl;
        nestedLoopJoin(inL, inR, out, sel, rec, runLen);
//     	cout<<"Join work nested end"<<endl;
        //nested join
    }
//    cout<<"Join work shutdown"<<endl;
    out->ShutDown();
}

void Join::Run(Pipe &inL, Pipe &inR, Pipe &out, CNF &selOp, Record &literal){
    PACK_ARGS6(param, &inL , &inR , &out , &selOp , &literal, runLen);
    pthread_create(&worker,NULL,work,param);
}


bool JoinBuffer::add(Record &addme){
	
//	cout<<"JoinBuffer add begin"<<endl;
    if((size+=addme.getLength())>capacity) return 0;
    else
        buffer[nrecords++].Consume(&addme);
    
//	cout<<"JoinBuffer add end"<<endl;
    return 1;
}

JoinBuffer::~JoinBuffer() {delete [] buffer;}

JoinBuffer::JoinBuffer(size_t npages):size(0), capacity(npages*PAGE_SIZE), nrecords(0){
    buffer = new  Record[PAGE_SIZE*npages/sizeof(Record*)]; 
}

void* WriteOut::work(void *param){
    UNPACK_ARGS3(Args, param, inPipe, outFile , mySchema);
    FOR_INPIPE(rec, inPipe)
    rec.Print(outFile,mySchema);
    END_FOR
    
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema){
    PACK_ARGS3(param, &inPipe, outFile, &mySchema);
    pthread_create(&worker,NULL,work,param);
}

void* GroupBy::work(void *param){
    //cout<<"group by work start"<<endl;
    UNPACK_ARGS5(Args, param, in, out, om, func, runLen);
    Pipe sort(PIPE_SIZE);
    BigQ bq(*in, sort, *om, runLen);
    Record cur, next;
    ComparisonEngine cmp;
    int sumint = 0;
    double sumdouble = 0;
    if (sort.Remove(&cur)) {
        
        func->Apply(cur, sumint, sumdouble);
        
        while (sort.Remove(&next)) {
            if (cmp.Compare(&cur, &next, om)) {
                //putgroup
		putGroup(cur,sumint,sumdouble,out,om);
                cur.Consume(&next);
                sumint = 0; sumdouble = 0;
                func->Apply(cur, sumint, sumdouble);
            }
            else{
                int curint = 0; double curdouble = 0;
                func->Apply(next, curint, curdouble);
                sumint += curint;
                sumdouble += curdouble;
            }
        }//putgroup
	putGroup(cur,sumint,sumdouble,out,om);
    }
    out->ShutDown();
    //cout<<"group by work end"<<endl;
}


void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
    PACK_ARGS5(param, &inPipe, &outPipe, &groupAtts, &computeMe, runLen);
    pthread_create(&worker,NULL,work,param);
}

void* Sum::work(void *param){
//	cout<<"sum work begin"<<endl;
    UNPACK_ARGS3(Args, param, in, out, func);
    int sumint = 0;
    double sumdouble = 0;
    Record rec;
    while(in->Remove(&rec)){
       	//cout<<"current in remove"<<endl;
        int curint = 0;
        double curdouble = 0;
	//cout<<"func apply begin"<<endl;
        if (Int == func->Apply(rec, curint, curdouble)) {
            sumint += curint;
//		cout<<sumint<<endl;
        }
        else if(Double == func->Apply(rec,curint,curdouble)){
            sumdouble += curdouble;
//		cout<<sumdouble<<endl;
        }
    //    cout<<"begin next remove"<<endl;
    }
//    cout<<"sum work out insert"<<endl;
	//cout<<"begin insert"<<endl;
        if (sumint != 0) {
            Record result(sumint);
	 //   cout<<"insert int"<<endl;
            out->Insert(&result);
        }
        else{
            Record result(sumdouble);
	  //  cout<<"insert double"<<endl;
            out->Insert(&result);
        }
    out->ShutDown();
//	cout<<"sum work end"<<endl;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    PACK_ARGS3(param, &inPipe, &outPipe, &computeMe);
    pthread_create(&worker,NULL,work,param);
}

void* DuplicateRemoval::work(void *param){
//    cout<<"DuplicateRemoval work begin"<<endl;
    UNPACK_ARGS4(Args, param, in, out, mySchema,runLen);
    OrderMaker om(mySchema);
    Pipe sort(PIPE_SIZE);
    BigQ biq(*in, sort, om, (int)runLen);
    Record cur,next;
    ComparisonEngine cmp;
    if (sort.Remove(&cur)){
        while (sort.Remove(&next)) {
            if (cmp.Compare(&cur, &next, &om)) {
                out->Insert(&cur);
                cur.Consume(&next);
            }
        }
        out->Insert(&cur);
    }
    out->ShutDown();
//    cout<<"DuplicateRemoval work end"<<endl;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
    PACK_ARGS4(param, &inPipe, &outPipe, &mySchema,runLen);
    pthread_create(&worker,NULL,work,param);
}

void* Project::work(void *param){
//    cout<<"Project work begin"<<endl;
    UNPACK_ARGS5(Args, param, in, out, keepMe, numAttsInput, numAttsOutput);
    int count = 0;
    FOR_INPIPE(rec, in)
       rec.Project(keepMe, numAttsOutput, numAttsInput);
       //cout<<"out Insert: "<<count++<<endl;
       out->Insert(&rec);
    END_FOR
//    cout<<"before ShutDown"<<endl;
    out->ShutDown();
//    cout<<"Project work end"<<endl;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
    PACK_ARGS5(param, &inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create(&worker,NULL,work,param);
}


void RelationalOp::WaitUntilDone(){
    pthread_join(worker, NULL);
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
    PACK_ARGS4(param,&inPipe, &outPipe, &selOp, &literal);
    pthread_create(&worker,NULL,work,param);
}

                

void* SelectPipe :: work(void* param){
//    cout<<"SelectPipe work begin"<<endl;
    UNPACK_ARGS4(Args, param, in, out, sel, lit);
    ComparisonEngine cmp;
    FOR_INPIPE(rec, in)
        if(cmp.Compare(&rec, lit, sel)) out->Insert(&rec);
    END_FOR
    out->ShutDown();
//    cout<<"Project work end"<<endl;
}
                
                void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal){
                    PACK_ARGS4(param, &inFile, &outPipe, &selOp, &literal);
                    pthread_create(&worker,NULL,work,param);
                }
                
                void* SelectFile::work(void *param){
//		    cout<<"selectFile work begin"<<endl;
                    UNPACK_ARGS4(Args, param, in, out, sel , lit);
                    Record next;
                    in->MoveFirst();
                    while(in->GetNext(next,*sel,*lit)) out->Insert(&next);
                    out->ShutDown();
//		    cout<<"selectFile work end"<<endl;
                }
                
                

                


