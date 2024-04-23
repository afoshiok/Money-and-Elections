docker run -d -p 3000:3000 \
  -e "MB_DB_TYPE=postgres" \
  -e "MB_DB_DBNAME=$MB_DB_DBNAME" \
  -e "MB_DB_PORT=$MB_DB_PORT" \
  -e "MB_DB_USER=$MB_DB_USER" \
  -e "MB_DB_PASS=$MB_DB_PASS" \
  -e "MB_DB_HOST=$MB_DB_HOST" \
  --name prod-metabase metabase/metabase
