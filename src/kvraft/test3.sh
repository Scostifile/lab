#for i in {1..10000}; do go test  -race; done
count=1000
    for i in $(seq $count); do
        printf "****************************************\n"
        go test
    done