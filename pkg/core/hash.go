package core

import "hash/fnv"

func Hash(value string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(value))
	return hash.Sum32()
}

func Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	return int(Hash(key)) % numPartitions
}
