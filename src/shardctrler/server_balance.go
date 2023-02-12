package shardctrler

func ifGidNeedHoldMoreShards(length int, subNum int, i int) bool {
	if i < length-subNum {
		return true
	}
	return false
}

func sortArray(Gid2ShardNumMap map[int]int) []int {
	length := len(Gid2ShardNumMap)

	orderedArray := make([]int, 0, length)
	for gid, _ := range Gid2ShardNumMap {
		orderedArray = append(orderedArray, gid)
	}

	// bubble sort
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if Gid2ShardNumMap[orderedArray[j]] < Gid2ShardNumMap[orderedArray[j-1]] ||
				(Gid2ShardNumMap[orderedArray[j]] == Gid2ShardNumMap[orderedArray[j-1]] &&
					orderedArray[j] < orderedArray[j-1]) { // ensure the uniqueness of sorting
				orderedArray[j], orderedArray[j-1] = orderedArray[j-1], orderedArray[j]
			}
		}
	}
	return orderedArray
}

func (sc *ShardCtrler) reBalanceShards(
	Gid2ShardNumMap map[int]int,
	lastShards [NShards]int,
) [NShards]int {
	length := len(Gid2ShardNumMap)
	average := NShards / length
	shardNumNeedReset := NShards % length // the number of shards which doesn't need to reset gid
	realSortNum := sortArray(Gid2ShardNumMap)

	// decrease larger shards
	for i := length - 1; i >= 0; i-- {
		resultNum := average
		if !ifGidNeedHoldMoreShards(length, shardNumNeedReset, i) {
			resultNum = average + 1
		}
		if resultNum < Gid2ShardNumMap[realSortNum[i]] {
			gidNum := realSortNum[i]
			changeNum := Gid2ShardNumMap[gidNum] - resultNum
			for shardIndex, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == gidNum {
					lastShards[shardIndex] = 0 // make room for smaller shards in fellow for{} loop
					changeNum -= 1
				}
			}
		}
		// increase smaller shards
		if resultNum > Gid2ShardNumMap[realSortNum[i]] {
			gidNum := realSortNum[i]
			changeNum := resultNum - Gid2ShardNumMap[gidNum]
			for shardIndex, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shardIndex] = gidNum
					changeNum -= 1
				}
			}
		}
	}
	return lastShards
}

func (sc *ShardCtrler) MakeMoveConfigL(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	for shardIndex, gidNum := range lastConfig.Shards {
		tempConfig.Shards[shardIndex] = gidNum
	}
	tempConfig.Shards[shard] = gid

	for gidIndex, serverList := range lastConfig.Groups {
		tempConfig.Groups[gidIndex] = serverList
	}

	return &tempConfig
}

func (sc *ShardCtrler) MakeJoinConfigL(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	for gid, serversList := range lastConfig.Groups {
		tempGroups[gid] = serversList
	}
	for gid, serversList := range servers {
		tempGroups[gid] = serversList
	}

	Gid2ShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		Gid2ShardNumMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			Gid2ShardNumMap[gid]++
		}
	}

	if len(Gid2ShardNumMap) == 0 {
		//fmt.Printf("%v: MakeJoinConfig GidToShardNumMap=0! tempGroups=%v\n", sc.me, tempGroups)
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(Gid2ShardNumMap, lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfigL(gids []int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	ifLeaveSet := make(map[int]bool)
	for _, gid := range gids {
		ifLeaveSet[gid] = true
	}

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for _, gidLeave := range gids {
		delete(tempGroups, gidLeave)
	}

	newShard := lastConfig.Shards // just shallow copy
	Gid2ShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			Gid2ShardNumMap[gid] = 0
		}
	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				Gid2ShardNumMap[gid]++
			}
		}
	}
	if len(Gid2ShardNumMap) == 0 {
		//fmt.Printf("%v: MakeLeaveConfig GidToShardNumMap=0! tempGroups=%v\n", sc.me, tempGroups)
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(Gid2ShardNumMap, newShard),
		Groups: tempGroups,
	}
}
