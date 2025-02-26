package main

import (
	server "jaypd/coordinator/coordinator"
	"time"
)

func main() {
	n, k := 12, 3
	coordinatorServer := server.NewCoordinator(n, k)
	coordinatorServer.Listen()
	args := []string{"dead.txt", "lorem.txt", "piggy.txt", "red.txt", "redemption.txt", "nothing.txt"}
	counter := 0
	for counter < len(args) {
		time.Sleep(10 * time.Second)

		coordinatorServer.FulfillRequest(args[counter])
		counter++

	}
}
