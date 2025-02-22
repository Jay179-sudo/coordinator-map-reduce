package main

import (
	server "jaypd/coordinator/coordinator"
)

func main() {
	n, k := 12, 3
	coordinatorServer := server.NewCoordinator(n, k)
	coordinatorServer.Listen()
	for {

	}
}
