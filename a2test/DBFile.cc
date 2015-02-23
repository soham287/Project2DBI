#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "assert.h"
#include "iostream"

// stub file .. replace it with your own DBFile.cc
File dbf;
off_t cpageindex;
Page cpage;

DBFile::DBFile ()
{}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
dbf.Open(0,f_path);
return 0;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	FILE *tf=fopen(loadpath,"r");
	if(tf==0)
		return;
	Record tr;
	Page tp;
	int rcounter=0;
	int pcounter=0;
	while(tr.SuckNextRecord(&f_schema,tf))
	{
		assert(pcounter>=0);
		assert(rcounter >=0);
		rcounter++;
		if(tp.Append(&tr)==0)
		{
			dbf.AddPage(&tp,pcounter++);
			tp.EmptyItOut();
			tp.Append(&tr);
		}
	}
	dbf.AddPage(&tp,pcounter++);	
}

int DBFile::Open (char *f_path) 
{
	dbf.Open(1,f_path);
	return 0;
}

void DBFile::MoveFirst () {
cout<<"Entered";
	cpageindex = (off_t) 0;
	if(dbf.GetLength() !=0)
	{
		dbf.GetPage(&cpage,cpageindex);
	}
	else
	{
		cpage.EmptyItOut();
	}
}

int DBFile::Close () {
	if(dbf.Close()>=0)
		return 1;
	else
		return 0;
}

void DBFile::Add (Record &rec) {
	Page tp;
	if(dbf.GetLength()!=0)
	{
		dbf.GetPage(&tp, dbf.GetLength()-2);
		if(tp.Append(&rec)==0)	
		{
			tp.EmptyItOut();
			tp.Append(&rec);
			dbf.AddPage(&tp,dbf.GetLength()-1);			
		}
		else
		{
			dbf.AddPage(&tp,dbf.GetLength()-1);	
		}
	}
	else
	{
		if(tp.Append(&rec)==1)
		{
			dbf.AddPage(&tp,0);
		}
	}
}

int DBFile::GetNext (Record &fetchme) {
	if(0 == cpage.GetFirst(&fetchme))
	{
		cpageindex++;
		if(cpageindex <= dbf.GetLength() - 2)
		{
			dbf.GetPage(&cpage, cpageindex);
			assert (1 == cpage.GetFirst(&fetchme));
			return 1;
		}
		else
		{
			return 0;
		}
	}
	else
	{
		return 1;
	}
	return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine c;
	while(GetNext(fetchme)==1)
	{
		if(c.Compare(&fetchme,&literal,&cnf)==1)
			return 1;
	}
	return 0;
}
