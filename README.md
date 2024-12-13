# distributed_systems_ml

Client: 
-main(): calls test
-test(): contains various input parameters (this is already in a dict), calls send_request
-send_request(): calls prepare_message and send_message
-get_payload(): if the message has a payload, create it here and attach it to the parameters
-send_message(): passes the message to the rpc. waits for a response and checks the ack flag to see if the response succeeded; checks the action_required flag to see if further action is needed (write_to_file)
-write_to_file(): called if the response requires the client to write a file to storage
-method name is both separate and in the header list

RPC:
-call(): Uses the client's socket address to create a connection to the server. Parses the method, headers, and payload from the dictionary and creates a Message object. checks the method and, depending on the method, may do additional processing to get more headers (ex. file size).  If there is a problem with the message, it returns an error to the client and closes the connection. Calls send()
-send(): sends the request to the server and waits for a response. passes the response to the client? 
-if the request did not succeed, does the client call the rpc again and the connection is closed and reopened?
-sends headers and  payload separately, using 2 sends?

Message:
-does error handling to make sure all parameters are included in the header
-headers are stored in a dictionary 
-payload is separate from the headers

Server: 
-start_server(): connection is initiated
-handle_client(): checks the method to decide which sub-function to call
-file_upload(): stores file. sends ack response
-file_retrieve(): retrieves file from dict. sends file back in payload to the rpc
-value_upload(): stores value. sends ack response
-value_retrieve(): retrieves value from dict. sends value back in payload to the rpc


TODO:
-Finish fixing up current code and test that everything works
-Start adding node classes (general node class, masternode, integrate with client and server classes, config files) 

Node:
-Uses config to initialize each node and start up client/server
-Iterate through config to initialize each node, starting with MasterNode (or just initialize MasterNode?)
MasterNode:
-Listen for heartbeats from workers to check that they're active 
-Track what the workers and roles are 
-Receive packets from workers about where data is stored and save it in a directory
-Receive requests searching for data and return a response with where the data is stored
-Use config to initialize each node and store it 
-Initialize JobManager
-Initialize a TaskManager for each WorkerClient
WorkerServer:
-Send heartbeats to MasterNode 
-Request for sending data and retrieving data location from MasterNode
WorkerClient:
-Send heartbeats to MasterNode 
-Request for retrieving data location from MasterNode
-Only data on the server-side is stored
JobManager:
-Is a part of MasterNode
-Has a queue of jobs 
-Has a dictionary of active tasks and their statuses
-Send task to worker
-Receive task update from worker
TaskManager:
-Is a part of WorkerNode
-Notifies the MasterNode if the worker is currently busy 
-Receive and execute tasks by calling the command on the workernode
-Report when the task is complete

MapReduce:
UserClient
-Initiates an RPC call to the MasterNode with MapReduce instructions
-Mapreduce instruction can be used with many files at once

MasterNode
-Decomposes data into chunks 
-Selects a node to be the reduce node
-Sends map tasks to worker clients
-Collects results from client
-Reduce node is either sent the results by each client node, or it retrieves the results itself

DataManager
-Execute map phase and reduce phase for data chunks 
-Saves results to file

JobManager

TaskManager

WorkerNode

WorkerServer

WorkerClient

Steps:
1. Add a new request type for mapreduce
2. When the userclient sees this task, it iterates through the files. For all files that are too big, it chunks these files and creates a new separate task. The instruction should include the associated task and sequence #, as well as total size? When chunking the file, a new file is created for each chunk
3. JobManager sees that a mapreduce task has been received. Jobmanager will randomly select a client for the reduce task
4. JobManager sends tasks to clients; each task contains 1 chunk
5. Clients run the map phase, which involves aggregating 

Map phase:
-Convert categorical variables into ordinal encodings
-Calculate averages for player stats 
-Create new derived features like points per minute and rebounds per minute
-Summarize team-level stats for each chunk (sum of points, rebounds, etc.)

Reduce phase:
-Aggregate new data and compute final averages

-team, position: list of players
-player:
    -salary
    -points (average per minute)
    -rebounds (average per minute)
    -assists (average per minute)
    -turnovers (average per minute)
    -minutes played (average per game)
    -games played (total)
    -free throw percentage (overall average)
    -field goal percentage (overall average)
    -three-point percentage (overall average)
    -injury status

map:
-For two columns, calculate the total and count, and then use this to calculate the average (ex. point per minute). Create a new column to put this data in
-For one column, calculate the total and count, and then calculate the overall average
-For one column, calculate the total sum
-Convert categorical variables into ordinal encodings
-Extract the team-player position and the name of the player
-If a value is missing, do not count it?

reduce:
-Generate player key-value aggregations, where each player has their total stats (for example, calculate total averages)
-Generate team-position aggregations, where each team-position key is paired with a list of the players in that team

Data:
Dataset 1 (season)
-Player name
-Team
-Position (G, F, C)
-Avg minutes 
-Avg points per min
-Avg rebounds per min
-Avg assists per min
-Avg steals per min
-Avg blocks per min
-Avg double-double per min
-Avg triple-double per min
-Avg 3PM per min
-Avg fantasy points per min

Dataset 2 (per game)
-Player name
-Team
-Position
-Opponent
-Injured (ignore)
-Home_away
-Total minutes
-Total points per min
-Total rebounds per min
-Total assists per min
-Total steals per min
-Total blocks per min
-Total double-double per min
-Total triple-double per min
-Total 3PM per min
-Total fantasy points per min
-Total games 

PLAYER_NAME
TEAM_ABBREVIATION
MATCHUP 
MIN
REB
AST
TOV
STL
BLK
PTS
NBA_FANTASY_PTS


# in the script, create multiple copies of all program files and move them in the config folders
# files should be saved in that folder

# check linear regression calculations 

# improve output

# script:
Python folder
Node program folder -- node1, node2, ...
Copy current node folder into image
1. node directories and job files
2. get the config files from the directories
3. pass to UserClient which initializes MasterNode; remove MasterNode initialization, move to script
4. MasterNode initializes each WorkerServer/WorkerClient, move to script

Deployment
1. Package the system for the image
2. Run the image
3. 1 image can be for master, 1 image for a worker node; can run the master and worker images, can run multiple worker images
4. 1 image for master node runs in 1 container, 1 image for worker node (run multiple in different containers with different exposed port) 
1 node --> 1 exposed port # --> 1 server, 3 clients (still keep internal port #s) 

(WorkerNode needs exposed port # of the MasterNode)

UserClient handles job, script is started at the same time
file1 (local) --> file1_part1 (local), part2 (local), part3
worker1 --> part1, worker2, worker3 
send_file() from local server to each worker (jobmanager can process)
and then store in their local


ClientScript send the files to each node

JobManager --> randomly selects worker, sends task
UserClient --> splits jobs into tasks (retrieve_file, map, reduce)



Script starts MasterNode in container, given exposed port # --> script
UserClient passes job to MasterNode
Script starts WorkerNodes, passes them MasterNode's port --> script
WorkerNodes report to MasterNode, Masternode stores them in registry --> Python



Workers to get masternode port --> script,


5. 

Master --> starts Workers, master passes port


1. Script starts WorkerServer/WorkerClient, and MasterNode
2. DataScript:
    -Split files into data chunks
    -Send chunk to workernode
    -Notify masternode
    
UserClient - splits files into chunks --> rename as Client
JobMaster - sends chunk to WorkerNode
DataManager - notifies MasterNode

2. JobScript --> SendFile on each chunk, 

