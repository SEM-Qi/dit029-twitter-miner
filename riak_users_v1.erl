-module(riak_users_v1).
-export([start/0, store/1, fetch/1, createUserObj/3]).


%% Everything is happening in an Erlang node with riak-erlang-client library
%% so you start it by 
%% erl -pa path_to_riac/ebin path_to_riac/deps/*/ebin
%% we register that node's pid so it could be used through out the all module 
start() ->
	case whereis(sts) of
		undefined ->
			{ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
			{register(sts, Pid), whereis(sts)};
		Pid -> {already_started_at, Pid}
	end.
	
%% store("accs.json") or any other json file. So far I used json file 
%% which looks similar to player info mentioned in Git/wiki/API

%% Bucket: User (ALWAYS THE SAME)
%% Key: User_email
%% Value: {Password, Name}

store(Link) ->
	{ok, File} = file:read_file(Link),
	[{[{_, User_email}|_]}] = jiffy:decode(File),
	[{[_,_,{_, Password}|_]}] = jiffy:decode(File),
	[{[_,{ _, Name}|_]}] = jiffy:decode(File),
	%% after decoding json file, object is created and put into Riak
	try
		Object = createUserObj(User_email, Password, Name),
		{riakc_pb_socket:put(sts, Object), stored_successfully}
	catch
		_:Reason -> {error_storing, Reason}
	end.

%% this one is for testing
%% its called fetch("mail").

fetch(Key) ->
	{ok, Object} = riakc_pb_socket:get(sts, <<"User">>, Key),
	Value = riakc_obj:get_value(Object),
	binary_to_term(Value).
	%term = tuple
	%string = list
	
		
createUserObj(User_email, Password, Name) -> 
	Value = {Password, Name},
	riakc_obj:new(<<"User">>,
	            User_email, 
	            term_to_binary(Value)).
	            	            
