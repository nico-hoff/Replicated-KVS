
echo PUTs
for i in $(seq 1 10)
do
	echo PUT Key/Value $i
	./task3-nico-hoff/build/dev/clt -p 2025 -o PUT -k $i -v $i
	echo returned $?
done

echo DIRECT_GETs
for i in $(seq 1 10)
do
	for j in $(seq 2025 2027)
	do
		./task3-nico-hoff/build/dev/clt -p $j -o DIRECT_GET -k $i
		echo Server $j - Key $i was $?.
	done
done
