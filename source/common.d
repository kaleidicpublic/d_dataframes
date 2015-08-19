module dataframe.common;
import std.conv;
import std.csv;
import std.datetime;
import std.exception;
import std.range:array, stride,only;
import std.stdio;
import std.variant;
import std.string:isNumeric;
alias KalVariant=Algebraic!(string,int,long, DateTime, float,double);
import std.typecons:tuple,Tuple;

debug=1;

enum ColumnType
{
	Int,
	Long,
	Double,
	Date,
	DateTime,
	String
}

alias KalType=string;

bool isDouble(KalVariant[] data)
{
	import std.string:strip,isNumeric;
	foreach(cell;data)
	{
		auto s=cell.to!string;
		if ((!s.strip.length==0) && (!s.isNumeric))
			return false;
	}
	return true;
}

bool isInteger(T=int)(KalVariant[] data)
if (is(T==int)||is(T==long))
{
	foreach(cell;data)
	{
		try
		{
			auto s=cell.get!string;
			auto z=(s).to!T;
		}
		catch(Exception e)
		{
			return false;
		}
	}
	return true;
}

bool isDate(T=Date)(KalVariant[] data)
if(is(T==Date)||is(T==DateTime)||is(T==SysTime))
{
	foreach(cell;data)
	{
		try
		{
			auto s=cell.get!DateTime;
			continue;
		}
		catch(Exception e)
		{

		}
		auto s=cell.get!string;
		if (s.isNumberDate)
			continue;
		try
		{
			auto z=SysTime.fromSimpleString(s);
			continue;
		}
		catch(Exception e)
		{
			return false;
		}
	}
	return true;
}


bool isNumberDate(string s)
{
	import std.string:strip;
	s=s.strip;
	if ((!s.isNumeric) || (s.length!=8))
		return false;
	auto y=s[0..4].to!int;
	auto m=s[4..6].to!int;
	auto d=s[6..8].to!int;
	if ((y>1900) && (m>=1)&&(m<=12)&&(d>=1)&&(d<=31))
		return true;
	return false;
}



Date dateTimeToDate(DateTime st)
{
	return std.datetime.Date(st.year,st.month,st.day);	
}
Date stringToDate(string s)
{
	auto st=SysTime.fromSimpleString(s);
	return std.datetime.Date(st.year,st.month,st.day);
}
DateTime stringToDateTime(string s)
{
	auto st=SysTime.fromSimpleString(s);
	return std.datetime.DateTime(st.year,st.month,st.day,st.hour,st.minute,st.second);
}

DateTime dateToDateTime(Date d)
{
	return std.datetime.DateTime(d.year,d.month,d.day,0,0,0);
}

bool isDashYYYYMMDD(string s)
{
	import std.string:strip,isNumeric;
	s=s.strip;
	if (s.length!=10)
		return false;
	if ((s[4]!='-') || (s[7]!='-'))
		return false;
	if ((!s[0..4].isNumeric)||(!s[5..7].isNumeric)||(!s[8..10].isNumeric))
		return false;
	auto m=(s[5..7]).to!int;
	if((m<1)||(m>12))
		return false;
	auto d=s[8..10].to!int;
	if((d<1)||(d>31))
		return false;
	return true;
}

Date parseDashYYYYMMDD(T:Date)(string s)
{
	import std.string:strip;
	s=s.strip;
	auto y=s[0..4].to!int;
	auto m=s[5..7].to!int;
	auto d=s[8..10].to!int;
	return std.datetime.Date(y,m,d);
}

DateTime parseDashYYYYMMDD(T:DateTime)(string s)
{
	import std.string:strip;
	s=s.strip;
	auto y=s[0..4].to!int;
	auto m=s[5..7].to!int;
	auto d=s[8..10].to!int;
	return std.datetime.DateTime(y,m,d,0,0,0);
}
Date parseDate(T:Date)(string s)
{
	if (s.isNumberDate)
		return s.numberDate!Date;
	if (s.isDashYYYYMMDD)
		return s.parseDashYYYYMMDD!Date;
	auto t=SysTime.fromSimpleString(s);
	return std.datetime.Date(t.year,t.month,t.day);
}

DateTime parseDate(T:DateTime)(string s)
{
	if (s.isNumberDate)
		return s.numberDate!DateTime;
	if (s.isDashYYYYMMDD)
		return s.parseDashYYYYMMDD!DateTime;
	auto t=SysTime.fromSimpleString(s);
	return std.datetime.DateTime(t.year,t.month,t.day,t.hour,t.minute,t.second);
}
DateTime numberDate(T:DateTime)(string s)
{
	import std.string:strip;
	s=s.strip;
	enforce((s.isNumeric) && (s.length==8));
	auto y=s[0..4].to!int;
	auto m=s[4..6].to!int;
	auto d=s[6..8].to!int;
	return std.datetime.DateTime(y,m,d,0,0,0);
}
Date numberDate(T:Date)(string s)
{
	import std.string:strip;
	s=s.strip;
	enforce((s.isNumeric) && (s.length==8));
	auto y=s[0..4].to!int;
	auto m=s[4..6].to!int;
	auto d=s[6..8].to!int;
	return std.datetime.Date(y,m,d);
}


string toString(KalVariant[] cells)
{
	string ret;
	foreach(cell;cells)
		ret~=cell.to!string~"\n";
	return ret;
}
void log(string s)
{
	writefln("%s",s);
	stdout.flush;
}

private size_t peekCols(string data, char separator=',')
{
	import std.string:indexOf,split;
	auto i=indexOf(data,"\n");
	return (i==-1)?0:(data[0..i].split([separator]).length);
}
private string[] peekHeaderCols(string data, char separator=',')
{
	import std.string:indexOf,split;
	auto i=indexOf(data,"\n");
	return (i==-1)?[]:(data[0..i].split([separator]));
}
