#include "BigQ.h"
#include "File.h"
#include <vector>
#include "iostream"
#include "ComparisonEngine.h"
#include "iostream"
#include <algorithm>
#include <pthread.h>
#include "Record.h"
struct testutil{
Pipe *in;
Pipe *out;
OrderMaker *sortorder;
int runlen;
};
Page page;
ComparisonEngine compareEngine;
OrderMaker *sortorderBuffer;
bool  compareRecords(Record* r1, Record* r2){

if(compareEngine.Compare(r1,r2,sortorderBuffer)==1){

return false;
}

return true;
}
void  putRunVectorIntoFile(vector<Record*> &recordVector,vector<Record*> &finalVector){
//std::sort (recordVector.begin(), recordVector.end(), compareRecords);	
	cout<<recordVector.size();
 for (std::vector<Record*>::iterator it=recordVector.begin(); it!=recordVector.end(); ++it) {
//t->out->Insert(*it);
finalVector.push_back(*it);
}
cout<<finalVector.size();
recordVector.clear();
 }
void  putPageIntoVector(vector<Record*> &recordVector, Page &dummyPage){
	Record  newRecord;
	Record *ptr;
	while(dummyPage.GetFirst(&newRecord)==1){
		ptr=&newRecord;
		recordVector.push_back(ptr);
		ptr= new Record();
	}
	cout<<recordVector.size();
}
void *  sortRecords(void *arg){
testutil *t = (testutil *) arg;
Record  *newRecord = new Record(); 
sortorderBuffer=t->sortorder;   
vector <Record*> recordVector;
vector <Record*> finalVector;
bool pipeEmpty=false;
while(t->in->Remove(newRecord)==1){
 int runlencount=0;
 while(runlencount!=t->runlen) {  // To insert runlength number of records in one go into the vector.
	 Page dummyPage;
	 while (dummyPage.Append(newRecord)==1) {
		newRecord= new Record();
		if(t->in->Remove(newRecord)==1){
			continue;
		}
		else { pipeEmpty=true;
			break;
		}
	 }
	 putPageIntoVector(recordVector,dummyPage);
	if (pipeEmpty==true) {
		break;
	}
 }
 putRunVectorIntoFile(recordVector,finalVector);
 
}
// construct priority queue over sorted runs and dump sorted data 
// into the out pipe
cout<<"HI"<<finalVector.size();
 /*for (std::vector<Record*>::iterator it=finalVector.begin(); it!=finalVector.end(); ++it) {
			t->out->Insert(*it);
}*/
// finally shut down the out pipe
t->out->ShutDown ();
}
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
// read data from in pipe sort them into runlen pages
pthread_t thread3;
testutil tutil ={&in,&out,&sortorder,runlen};
pthread_create (&thread3, NULL, sortRecords, (void *)&tutil);
pthread_join (thread3, NULL);

}

BigQ::~BigQ () {
}
