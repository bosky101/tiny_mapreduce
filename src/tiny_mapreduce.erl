%%%-------------------------------------------------------------------
%%% File    : tiny_mapreduce.erl
%%% Author  : bosky101
%%% Description : simple erlang implementation of map/reduce
%%% (that accepts user defined Functions )
%%% see http://github.com/bosky101/tiny_mapreduce/tree/master
%%%
%%%-------------------------------------------------------------------
-module(tiny_mapreduce).

-export([
	 map/2,
	 intermediate/1,
	 reduce/2,

	 test/0,min/0
	]).

%%%====================================================================
%%% API
%%%====================================================================

%%--------------------------------------------------------------------
%% Function: Map
%% Description: Takes an input list of {key1,value1} pairs and applies a
%% user-defined function to each produce a new list of intermediate
%% {key2,value2} pairs 
%%--------------------------------------------------------------------
map(List,UserMapFn)->
    lists:map (
      fun(Key1Val1)->
	     applyUserFunction(UserMapFn,Key1Val1)	     
      end,
      List).

%%--------------------------------------------------------------------
%% Function: InterMediate
%% Desciption: Takes the intermediate list of {key2,value2} pairs and 
%% group together all values associated with the same intermediate key
%% to produce a list of {key3,value3} pairs of equal or shorter length
%%--------------------------------------------------------------------
intermediate(List)->
    dict:to_list(
      lists:foldl( fun({Key2,Val2},InterMediateAcc)-> 
			   dict:append(Key2,Val2,InterMediateAcc)
		   end,	   dict:new(),List)
     ). 

%%--------------------------------------------------------------------
%% Function: Reduce
%% Desciption: Takes the resulting intermediate list of {key3,value3}
%% pairs , accepts each intermediate key, and its set of values,
%% possibly merges them,  to form a smaller ,equal or empty final list.
%%--------------------------------------------------------------------
reduce(List,UserReduceFn)-> 
    {Keys,_Vals} = lists:unzip(List),
    lists:zip(
      Keys,
      lists:map( fun(Key3Val3) -> 
			 applyUserFunction(UserReduceFn,Key3Val3)			
		 end,List)
     ).     

%%====================================================================
%% Internal functions
%%====================================================================

%% helper to support M/F/A M/F or singleton styled based 
%% user defined function
applyUserFunction(UserFn,Key3Val3)->
    case UserFn of
	{M,F,[]}->
	    erlang:apply(M,F,[Key3Val3]);
	{M,F,A}->
	    erlang:apply(M,F,[A,Key3Val3]);
	{M,F}->
	    erlang:apply(M,F,[Key3Val3]);
	Fn ->
	    Fn(Key3Val3)
    end.


test()->
    Eg1Input    = [a,b,c,d,a],
    Eg1MapFn    = fun(a)->{a,"apple"};(b)->{b,"ball"};(c)->{c,"cat"};(d)->{d,"dog"};(e)->{e,"emul"};(_Else)->"unexpected" end ,
    Eg1ReduceFn = fun({_K,V})-> lists:foldl( fun(_Char,Ctr)-> Ctr+1  end , 0, V) end,

    %% count occurances of a set of words
    Eg1 = ?MODULE:reduce( ?MODULE:intermediate ( ?MODULE:map(Eg1Input , Eg1MapFn)) , Eg1ReduceFn),
    
    Eg2Input    = [
		   {slot1,"11"},
		   {slot1,"1111"},
		   {slot2,"2"},
		   {slot3,"3"},
		   {slot3,"333"},
		   {slot3,"2"},
		   {slot2,"22"},
		   {slot1,"11"}
		   ],
    Eg2MapFn    = fun({Slot,X})-> 
			  case string:to_integer(X) of
			      {error,_}->
				  0;
			      {Numb,_}->
				  {Slot,Numb}
			  end
		  end,
    Eg2ReduceFn = fun({_K,V})-> lists:foldl( fun(X,Max) when Max=:= -1  -> X;(X,Max)-> 
						     case Max > X of
							 true-> Max;
							 _ -> X
						     end
					     end , -1, V) end,

    %% find max in different slots
    Eg2 = ?MODULE:reduce( ?MODULE:intermediate ( ?MODULE:map(Eg2Input , Eg2MapFn)) , Eg2ReduceFn),
    
    [
     Eg1, %%[{a,2},{b,1},{c,1},{d,1}]
     Eg2  %%[{slot1,1111},{slot2,22},{slot3,333}]
    ].
    
min()->
 Map2 = fun(L,Fn)-> lists:map (fun(A)-> Fn([A]) end, L) end,
 PostMap2 = fun(L)->dict:to_list(lists:foldl( fun({K,V},InterMed)-> dict:append(K,V,InterMed) end,dict:new(),L)) end,
 Output3 = fun(_K,V)-> lists:foldl( fun(_X,Ctr)-> Ctr+1  end , 0, V) end,
 Reduce4 = fun(L,Fn)-> {Keys,_Vals} = lists:unzip(L),lists:zip(Keys,lists:map( fun({InterMedKey,V}) -> Fn(InterMedKey,V) end,L)) end,
 Words2 = fun([a])->{a,"apple"};([b])->{b,"ball"};([c])->{c,"cat"};([d])->{d,"dog"};([e])->{e,"emul"} end ,
 Reduce4( PostMap2 ( Map2([a,b,c,d,a] , Words2)) , Output3).
 
 
    

