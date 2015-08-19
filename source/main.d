module dataframe.main;
import dataframe.dataframe;
import hdf5.hdf5;

import std.exception;
import std.stdio;
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

auto splitLongTicker(string longTicker)
{
	import std.string:split;
	Tuple!(string,"exchange",string,"ticker") ret;
	auto tmp=longTicker.split("/");
	enforce(tmp.length==2);
	ret.exchange=tmp[0];
	ret.ticker=tmp[1];
	return ret;
}


void main1(string[] args)
{
	import std.string:toLower;
	enforce(args.length==3);
	auto splitTicker=splitLongTicker(args[2]);
	auto readFrame=dataFrameTypedFromSimpleHDF5Array(args[1],splitTicker.exchange, splitTicker.ticker,
		["Year","Month","Day","Open","High","Low","Close","Volume","Adj Close"]);
	writefln("%s",readFrame.toString);
	//writefln("%s,%s",readFrame[0].Year,readFrame[0].Close);
	//readFrame[0].Year=99999;
	//writefln("%s,%s",readFrame[0].Year,readFrame[0].Close);
	//double d=readFrame[0].Close;
	//writefln("%s",d);
	//writefln("%s/%s",readFrame.loadCell!double(0,"Month"), readFrame.loadCell!double(0,"Close"));
	//writefln("%s/%s",readFrame[0].loadCell!double("Month"),readFrame[0].loadCell!double("Close"));
}


void main2(string[] args)
{
	import std.string:toLower;
	//string fn="/hist/daily/eoddata/WCE_20150721.txt";
	//if (args.length>=2)
	//	fn=args[1];
	//auto titles=["Ticker","Date","Open","High","Low","Close","Volume","Open Interest"];
	//auto typedFrame=typedFrameFromCSV(fn,titles);
	//typedFrame=typedFrame.mergeFrames(typedFrameFromCSV("/hist/daily/eoddata/WCE_20150720.txt",titles));

/+	DataFrame frame;
	frame=frame.loadCSVFile(fn,true);
	//writefln("%s",frame.toString);
	auto types=frame.findColumnTypes;

	auto peek=(cast(string)std.file.read(fn)).peekCols;
	DataFrameTyped typedFrame;
	auto titles=["Ticker","Date","Open","High","Low","Close","Volume","Open Interest"];
	/*ColumnType[] columnTypes;
	with(ColumnType)
		columnTypes=[String,String,Double,Double,Double,Double,Int,Int];*/
	if (peek<titles.length)
	{
		titles.length=peek;
		//columnTypes.length=peek;
		writefln("shortening to %s cols", peek);
	}
	typedFrame=typedFrame.setColumnTitles(titles)
				.setColumnTypes(types) // columnTypes
				.loadCSVFile(fn,titles,true);+/
	//writefln("%s", typedFrame);
	//typedFrame.saveCSV("temp.csv",true);
//	writefln("%s",frame.toString);
	//writefln("%s",frame.findColumnTypes);
	//auto fn="/hist/hdf5/eod_FOREX.h5";
	enforce(args.length>=2);
	auto fn=args[1];
	//auto series="AUDUSD";
	enforce(args.length>=3);
	auto destFn=args[2];
	//writefln("%s",dataTypesForHDF5(fn,series));
	//writefln("%s",dataTypesForHDF5("/hist/hdf5/equitymain.hdf5","NASDAQ/AAPL"));
	if(args.length==4)
	{
			auto readFrame=dataFrameTypedFromHDF5DataSet(fn,args[3]);
			readFrame.toHDF5(destFn,args[3]);
	}
	else
	{
		foreach(ticker;fn.hdf5Contents)
		{
			if ((ticker==".") || (ticker==".."))
				continue;
			try
			{
				auto readFrame=dataFrameTypedFromHDF5DataSet(fn,ticker);
				readFrame.toHDF5(destFn,ticker);
			}
			catch(Exception e)
			{
				stderr.writefln("* error - skipping %s:%s",ticker,e.msg);
			}
			//writefln("%s",readFrame.toString);
			//readFrame.saveCSV(series.toLower~".csv",true);
		//readFrame.deleteColumn("openInterest");
		}
	}
	//auto h5=dataFrameTypedFromHDF5DataSet("/hist/hdf5/bb/equityindex/IndexTickers.h5","CAC Index");
	//writefln("%s",h5.toString);
}

void main(string[] args)
{
	writefln("dummy main");
}