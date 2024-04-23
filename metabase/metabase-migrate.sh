export MB_DB_TYPE=postgres
export MB_DB_CONNECTION_URI="$MB_DB_CONNECTION_URI"
java -jar metabase.jar migrate release-locks
java -jar metabase.jar load-from-h2 ./metabase.db 
