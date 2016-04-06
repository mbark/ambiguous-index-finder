LOADFILES=loadfiles/*
PGLOADER=$1
options=( "--verbose" "--debug" )
for f in $LOADFILES
do
    command=( "$PGLOADER" --verbose --debug "$f" )
    echo "Executing command ${command[@]}"
    "${command[@]}" &
done
wait
echo "We are done!"
