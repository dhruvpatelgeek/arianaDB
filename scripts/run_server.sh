#PORT NUMBERS------------------------------------
DATABASE_PORT=3200
SERVER_PORT=3001
#################################################
printf "BUILD STARTED @"
date
printf "[PROCRESS STARTED]*********************\n"
#     args [client port] [database port]
go run src/server/main.go $SERVER_PORT $DATABASE_PORT
printf "\n[PROCRESS EXIT]-----------------------\n"
