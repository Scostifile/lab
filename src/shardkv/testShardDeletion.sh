#for i in {1..10000}; do go test  -race; done
count=100
    for i in $(seq $count); do
        printf "****************************************\n"
        go test -run TestChallenge1Delete -race
    done