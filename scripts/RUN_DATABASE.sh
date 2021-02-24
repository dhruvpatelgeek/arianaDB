#PORT NUMBERS------------------------------------
DATABASE_PORT=3200
SERVER_PORT=3001
#################################################

printf "BUILD STARTED @"
date
printf "[PROCRESS STARTED]*********************\n"

go run mock_store/store.go $DATABASE_PORT
printf "\n[PROCRESS EXIT]-----------------------\n"
