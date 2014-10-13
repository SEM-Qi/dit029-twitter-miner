%ReWrite as OTP server

-module(twitterminer_riak).

-export([riakSocket/1, riakPut/1]).

% REFERENCE
% putTweet(ID, Tweet, Tag) ->
% 	Obj = riakc_obj:new(<<"Tweets">>,
%                     <<ID>>,
%                     <<"...user data...">>,
%                     <<"text/plain">>),
% 	MD1 = riakc_obj:get_update_metadata(Obj),
% 	MD2 = riakc_obj:set_secondary_index(
%     	MD1,
%     	[{{binary_index, "twitter"}, [<<"jsmith123">>]},
%     	 {{binary_index, "email"}, [<<"jsmith@basho.com">>]}]),
% 	Obj2 = riakc_obj:update_metadata(Obj, MD2),
% 	riakc_pb_socket:put(Pid, Obj2).


% Build Socket
riakSocket(IP) -> 
	try riakc_pb_socket:start_link(IP, 8087) of
    	{ok, RiakPID} ->
    		RiakPID
  	catch
    	_ ->
      		erlang:error(no_riak_connection)
  	end.

% Get tag and attached data via message as it occurs create an object

riakPut(SocketPID) ->
        receive
        	{store, Tag, CoTags, Tweet} -> 
          		Object = createRiakObj(Tag, CoTags, Tweet),
          		TagObject = addTagIndex(Object),
          		putRiakObj(SocketPID, TagObject),
          		deleteOld(SocketPID, Tag),
          		riakPut(SocketPID);
            close -> 
            	riakc_pb_socket:stop(SocketPID)          
        end.


% Parse it using extract??

% create riak object with following
% 	Bucket - Hashtag
% 	Key - ? autogenerate?
% 	Value - all data? - specifically other tags used?
createRiakObj(Tag, CoTags, Tweet) -> 
	io:format("Creating Object ~n"),
	Value = {CoTags, Tweet},
	riakc_obj:new(Tag,
	            undefined, %% undefined autogenerates keys
	            term_to_binary(Value)).
% insert 2i
% 	2i - timestamp (seconds since epoch)
addTagIndex(Object) ->
	io:format("Adding Meta Data ~n"),
	MD1 = riakc_obj:get_update_metadata(Object),
	MD2 = riakc_obj:set_secondary_index(MD1, [{{integer_index, "timestamp"}, [timeStamp()]}]),
	riakc_obj:update_metadata(Object, MD2).


putRiakObj(SocketPID, TagObject) ->
	io:format("Putting Data in ~n"),
	riakc_pb_socket:put(SocketPID, TagObject).

% delete from riak all tags with timestamps older than 20 (or whatever?) min.
% 	try to implement streaming? Probably only if super slow, 
% 	i believe it sends keys as messages? so handle as receive?
deleteOld(SocketPID, Tag) -> 
	io:format("Finding old Data ~n"),
	{ok, {_,Results,_,_}} = riakc_pb_socket:get_index_range(
    	SocketPID,
    	Tag, %% bucket name
    	{integer_index, "timestamp"}, %% index name
    	1413218244414109, oldTimeStamp() %% origin timestamp should eventually have some logic attached
		),
	io:format("Deleting old Data ~n"),
	[riakc_pb_socket:delete(SocketPID, Tag, X) || X <- Results],
	ok.
	
timeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

oldTimeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + ((Secs - 1200) * 1000 * 1000) + Micro.


	% {ok, Results} = riakc_pb_socket:get_index_range(Pid, <<"GonzaloHiguain">>, {integer_index, "timestamp"},1413218244414109, 1413236001036936)