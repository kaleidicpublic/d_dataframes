module dataframe.csv;
import dataframe.common;
import dataframe.variant;
import dataframe.typed;
import std.conv;
import std.csv;
import std.datetime;
import std.exception;
import std.range:array, stride,only;
import std.stdio;
import std.variant;
import std.string:isNumeric;
import std.typecons:tuple,Tuple;


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

DataFrame loadCSV(Malformed errorLevel=Malformed.ignore)(string data, bool hasHeader=false, char separator=',')
{
//		auto csv=csvReader!(string, errorLevel)(data, separator, quote);
	DataFrame ret;
	auto csv=csvReader(data,separator); // , separator, quote);
	size_t i;
	ret.setCellColumns(data.peekCols-1); // since first col is the index value
	if (hasHeader)
	{
		auto cols=peekHeaderCols(data,separator);
		ret.setColumnTitles(cols[1..$]);
		ret.setIndexTitle(cols[0]);
	}
	bool firstRow=true;
	foreach(row;csv)
	{
		if (firstRow && hasHeader)
		{
			firstRow=false;
			continue;
		}
		size_t j;
		foreach(col;row)
		{
			if (j==0)
				ret.indexValues~=col.to!KalVariant;
			else
			{
				ret.cellValues.length+=ret.numCols;
				ret[i,j]=col;
			}
			++j;
		}
		++i;
	}
	return ret;
}

DataFrame saveCSV(DataFrame frame, string filename, bool useHeader=true)
{
	string ret;
	if(useHeader)
	{
		foreach(j;0..frame.numCols)
			ret~=frame.columnTitles[j]~",";
		ret=ret[0..$-1]~"\n";
	}		
	foreach(i;0..frame.numRows)
	{
		string rowString="";
		foreach(j;0..frame.numCols)
			rowString~=frame[i,j].to!string~",";
		rowString=rowString[0..$-1]~"\n";
		ret~=rowString;
	}
	std.file.write(filename,ret);
	return frame;
}



DataFrameTyped loadCSV(Malformed errorLevel=Malformed.ignore)(string data, string[] columnTitles=[], bool skipFirst=false,char separator=',')
{
	import std.string:strip;
	import std.math:nan;
	import std.range:enumerate;
	DataFrameTyped ret;

//		auto csv=csvReader!(string, errorLevel)(data, separator, quote);
	auto csv=csvReader(data,separator); // , separator, quote);
	size_t i;
	auto peek=data.peekCols;
	bool hasHeader=(columnTitles.length==0);
	if (hasHeader)
		ret.setColumnTitles(peekHeaderCols(data,separator));
	else
	{
		ret.setColumnTitles(columnTitles);
		enforce(peek==columnTitles.length);
	}
	bool firstRow=true;

	foreach(row;csv)
	{
		if (firstRow && (hasHeader || skipFirst))
		{
			firstRow=false;
			continue;
		}
		size_t j;
		ret.length=ret.length+1;
		bool f=false;
		foreach(k,col;enumerate(row))
		{
			if(k==0)
				continue;
			if(col.strip.length>0)
			{
				if(col.isNumeric)
				{
					if(col.to!double!=0.0)
					{
						f=true;
						break;
					}
				}
				else
				{
					f=true;
					break;
				}
			}
		}
		if (!f)
			continue;
		foreach(col;row)
		{
			string colName=columnTitles[j];
			final switch(ret.columnTypes[colName]) with(ColumnType)
			{
				case String:
					ret[i,colName]=col;
					break;
				case Int:
					ret[i,colName]=col.to!int;
					break;
				case Long:
					ret[i,colName]=col.to!long;
					break;
				case ColumnType.Date:
					ret.values.dates[colName][i]=parseDate!(std.datetime.Date)(col);
					break;
				case ColumnType.DateTime:
					ret.values.dateTimes[colName][i]=parseDate!(std.datetime.DateTime)(col);
					break;
				case Double:
					ret[i,colName]=(col.strip.length==0)?double.nan:col.to!double;
					break;
			}
			++j;
		}
		++i;
	}
	return ret;
}

DataFrameTyped saveCSV(DataFrameTyped frame, string filename, bool useHeader=true)
{
	string ret;
	foreach(j;0..frame.numCols)
		ret~=frame.columnTitles[j]~",";
	ret=ret[0..$-1]~"\n";
	
	foreach(i;0..frame.numRows)
	{
		string rowString="";
		foreach(j;0..frame.numCols)
			rowString~=frame.loadCell!string(i,frame.columnTitles[j])~",";
		rowString=rowString[0..$-1]~"\n";
		ret~=rowString;
	}
	std.file.write(filename,ret);
	return frame;
}

