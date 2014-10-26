%ReWrite as OTP server

-module(twitterminer_riak).

-export([riakSocket/1, riakPut/4]).


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

riakPut(SocketPID, DeleteSocketPID, Count, IP) when Count > 100 ->
  receive
    {store, Tag, CoTags, Tweet} -> 
        NewDeleteSocketPID = riakSocket(IP),
        spawn(fun() -> createRiakObj(Tag, CoTags, Tweet, SocketPID) end),
        spawn(fun() -> deleteOld(DeleteSocketPID, Tag, stop) end),
        riakPut(SocketPID, NewDeleteSocketPID, 0, IP);
    close -> 
        riakc_pb_socket:stop(SocketPID);
    _ -> ok          
  end;

riakPut(SocketPID, DeleteSocketPID, Count, IP) ->
  receive
  	{store, Tag, CoTags, Tweet} -> 
    		spawn(fun() -> createRiakObj(Tag, CoTags, Tweet, SocketPID) end),
    		spawn(fun() -> deleteOld(DeleteSocketPID, Tag) end),
        riakPut(SocketPID, DeleteSocketPID, Count + 1, IP);
    close -> 
      	riakc_pb_socket:stop(SocketPID);
    _ -> ok          
  end.


% Parse it using extract??

% create riak object with following
% 	Bucket - Hashtag
% 	Key - ? autogenerate?
% 	Value - all data? - specifically other tags used?
createRiakObj(Tag, CoTags, Tweet, SocketPID) -> 
  Value = {CoTags, Tweet},
  Obj = riakc_obj:new(Tag,
          undefined, %% undefined autogenerates keys
          term_to_binary(Value)),
  addTagIndex(Obj, SocketPID),
  ok.

% insert 2i
% 	2i - timestamp (seconds since epoch)
addTagIndex(Object, SocketPID) ->
	MD1 = riakc_obj:get_update_metadata(Object),
	MD2 = riakc_obj:set_secondary_index(MD1, [{{integer_index, "timestamp"}, [timeStamp()]}]),
	TagObj = riakc_obj:update_metadata(Object, MD2),
  putRiakObj(SocketPID, TagObj),
  ok.


putRiakObj(SocketPID, TagObject) ->
  case process_info(SocketPID) of
    undefined ->
      io:format("The Socket died before put");
    _ ->
      riakc_pb_socket:put(SocketPID, TagObject),
      ok
  end.

% delete from riak all tags with timestamps older than 20 (or whatever?) min.
% 	try to implement streaming? Probably only if super slow, 
% 	i believe it sends keys as messages? so handle as receive?
%   First call is to stop and start the socket (Memory isssues)

deleteOld(DeleteSocketPID, Tag, stop) -> 
  case process_info(DeleteSocketPID) of
    undefined ->
      io:format("The Socket died before delete");
    _ ->
      {ok, {_,Results,_,_}} = riakc_pb_socket:get_index_range(
          DeleteSocketPID,
          Tag, %% bucket name
          {integer_index, "timestamp"}, %% index name
          1413218244414109, oldTimeStamp() %% origin timestamp should eventually have some logic attached
        ),
      [riakc_pb_socket:delete(DeleteSocketPID, Tag, X) || X <- Results],
      riakc_pb_socket:stop(DeleteSocketPID),
      ok
  end.

deleteOld(DeleteSocketPID, Tag) -> 
  case process_info(DeleteSocketPID) of
    undefined ->
      io:format("The Socket died before delete");
    _ ->
      {ok, {_,Results,_,_}} = riakc_pb_socket:get_index_range(
          DeleteSocketPID,
          Tag, %% bucket name
          {integer_index, "timestamp"}, %% index name
          1413218244414109, oldTimeStamp() %% origin timestamp should eventually have some logic attached
        ),
      [riakc_pb_socket:delete(DeleteSocketPID, Tag, X) || X <- Results],
      ok
  end.

	
timeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

oldTimeStamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + ((Secs - 1200) * 1000 * 1000) + Micro.
