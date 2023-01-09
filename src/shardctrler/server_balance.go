package kvraft

func ifAvg(length int, subNum int, i int) bool {
	if i < length-subNum {
		return true
	}
	return false
}
