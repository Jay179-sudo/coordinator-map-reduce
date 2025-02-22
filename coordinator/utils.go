package server

import "hash/fnv"

const NUMBER_OF_SERVERS = 12

func hash(fileName string, servers int) int {
	h := fnv.New32a()
	h.Write([]byte(fileName))
	return int(h.Sum32() % uint32(servers))
}
