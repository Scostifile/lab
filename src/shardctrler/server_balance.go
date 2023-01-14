package shardctrler

func ifReachBalanceSize(length int, subNum int, i int) bool {
	if i < length-subNum {
		return true
	}
	return false
}

func sortGidNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	// bubble sort
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] ||
				(GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] &&
					numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1], numArray[j]
			}
		}
	}
	return numArray
}

func (sc *ShardCtrler) reBalanceShards(
	GidToShardNumMap map[int]int,
	lastShards [NShards]int,
) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	subNum := NShards % length
	realSortNum := sortGidNumArray(GidToShardNumMap)

	// reduce large shards
	for i := length - 1; i >= 0; i-- {
		resultNum := average
		if !ifReachBalanceSize(length, subNum, i) {
			resultNum = average + 1
		}
		if resultNum < GidToShardNumMap[realSortNum[i]] {
			gidNum := realSortNum[i]
			changeNum := GidToShardNumMap[gidNum] - resultNum
			for shardIndex, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == gidNum {
					lastShards[shardIndex] = 0 // make room for smaller shards in fellow for() loop
					changeNum -= 1
				}
			}
			GidToShardNumMap[gidNum] = resultNum
		}
	}

	// increment smaller shards
	for i := 0; i < length; i++ {
		resultNum := average
		if !ifReachBalanceSize(length, subNum, i) {
			resultNum = average + 1
		}
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			gidNum := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[gidNum]
			for shardIndex, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shardIndex] = gidNum
					changeNum -= 1
				}
			}
			GidToShardNumMap[gidNum] = resultNum
		}
	}
	return lastShards
}

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
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

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	for gid, serversList := range lastConfig.Groups {
		tempGroups[gid] = serversList
	}
	for gid, serversList := range servers {
		tempGroups[gid] = serversList
	}

	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}
	}

	if len(GidToShardNumMap) == 0 {
		//fmt.Printf("%v: MakeJoinConfig GidToShardNumMap=0! tempGroups=%v\n", sc.me, tempGroups)
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {
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
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}
	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}
	}
	if len(GidToShardNumMap) == 0 {
		//fmt.Printf("%v: MakeLeaveConfig GidToShardNumMap=0! tempGroups=%v\n", sc.me, tempGroups)
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, newShard),
		Groups: tempGroups,
	}
}
