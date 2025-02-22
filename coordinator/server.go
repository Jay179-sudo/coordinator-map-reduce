package server

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const REBALANCE_NOJOBS_TIMEOUT time.Duration = 5 * time.Second
const REBALANCE_TIMEOUT time.Duration = 10 * time.Second

type Job struct {
	Filename string
}
type ServerInformation struct {
	Address     string    // address of the server
	Jobs        []Job     // queue of jobs (we are batching queries)
	LastUpdated time.Time // last healthcheck time. Stored in UTC
	SendJob     bool      // should you send a job here (may have crashed or something)
}
type CoordinationServer struct {
	mu      sync.Mutex          // for locking when goroutines from healthchecks update information
	Address string              // net address of the server
	Hasher  []ServerInformation // array where server information is stored
	Servers int                 // total number of servers to be accomodated
}

func NewCoordinator(n, k int) *CoordinationServer {
	return &CoordinationServer{
		mu:      sync.Mutex{},
		Address: "localhost:8880",
		Hasher:  make([]ServerInformation, n),
		Servers: n,
	}
}

// create a new RPC server
// start the RPC server in a separate goroutine
// should have the /register endpoint for servers to ask for Node Address
// should have a /healthcheck endpoint for servers to do a healthcheck
func (server *CoordinationServer) Listen() {

	err := rpc.Register(server)
	if err != nil {
		fmt.Println("Error Detected, exiting process ", err)
		return
	}
	listener, err := net.Listen("tcp", server.Address)
	if err != nil {
		fmt.Println("Error Detected, exiting process. Could not start TCP Listener")
		return
	}
	go func() {
		rpc.Accept(listener)
	}()
	// start rebalancing queues (background thread)
	go server.rebalanceQueues()

}

func (server *CoordinationServer) CallRequest(workerId int, reply *int) error {
	// dummy rpc check
	*reply = 55
	fmt.Println("Server called!")
	return nil
}

func (server *CoordinationServer) Register(workerAddr string, reply *int) error {

	server.mu.Lock()

	count := 0
	hashed := hash(workerAddr, server.Servers) // do you hash again or do you increment by 1?
	for {

		if server.Hasher[hashed].Address == "" {
			*reply = hashed

			server.Hasher[hashed].Address = workerAddr
			server.Hasher[hashed].SendJob = true
			server.Hasher[hashed].LastUpdated = time.Now().UTC()

			break
		} else {
			if count == server.Servers {
				// no slot empty
				return errors.New("no slot found") // return an error saying not slot empty
			}
		}
		hashed++
		count++
	}
	server.mu.Unlock()

	return nil
}

func (server *CoordinationServer) Healthcheck(workerId int, reply *int) error {
	server.mu.Lock()

	time := time.Now().UTC()
	server.Hasher[workerId].LastUpdated = time

	fmt.Printf("Received healthcheck by worker: %v. Last Updated on: %v\n", workerId, server.Hasher[workerId].LastUpdated)

	server.Hasher[workerId].SendJob = true
	server.mu.Unlock()

	return nil
}

// does this have to have server *CoordinationServer?
// this is when you react to user http input
func (server *CoordinationServer) FulfillRequest(fileName string, reply *int) error {

	count := 0
	position := hash(fileName, server.Servers)
	for {

		if server.Hasher[position].SendJob {
			server.Hasher[position].Jobs = append(server.Hasher[position].Jobs, Job{Filename: fileName})
			return nil
		} else {
			if count == server.Servers {
				return errors.New("no slot found")
			}
		}
		position++
		count++

	}

}

func (server *CoordinationServer) RequestJobs(workerAddress string, reply *[]Job) error {
	hashed := hash(workerAddress, server.Servers)
	jQueue := make([]Job, len(server.Hasher[hashed].Jobs))
	*reply = jQueue
	// yo wala should be the same as registration function?
	return nil
}

func (server *CoordinationServer) rebalanceQueues() {
	// look into last updated time and decide to rebalance queue
	go func() {
		// run infinitely, trying to rebalance queues
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				server.mu.Lock()

				for key, value := range server.Hasher {

					if value.Address == "" {
						continue
					}

					if time.Since(value.LastUpdated) >= REBALANCE_TIMEOUT {
						// send to neighbours

						size := len(server.Hasher[key].Jobs)
						if size == 0 {
							server.Hasher[key].Address = ""
							continue
						}
						// remove from this array and distribute half to key - 1 and distribute the rest to key + 1

						leftServer := key - 1

						for {

							if leftServer == key {
								break
							}

							if server.Hasher[leftServer].SendJob {
								break
							} else {
								leftServer = (leftServer - 1 + server.Servers) % server.Servers
							}

						}

						rightServer := key + 1

						for {

							if rightServer == key {
								break
							}
							if server.Hasher[rightServer].SendJob {
								break
							} else {
								rightServer = (rightServer + 1) % server.Servers
							}

						}
						for i := 0; i < size/2; i++ {
							// also check if tyo wala server ni down cha ki nai. SendJob should be true
							server.Hasher[leftServer].Jobs = append(server.Hasher[leftServer].Jobs, server.Hasher[key].Jobs[i])

						}

						for i := size / 2; i < size; i++ {
							// also check tyo wala server ni down cha ki nai. SendJob should be false
							server.Hasher[rightServer].Jobs = append(server.Hasher[rightServer].Jobs, server.Hasher[key].Jobs[i])

						}
						fmt.Println("Rebalanced...")
						server.Hasher[key].Jobs = []Job{}
						server.Hasher[key].Address = ""

						// have some way to create a server?
						// server.Hasher[key].SendJob = true //only do this after restart?
					} else if time.Since(value.LastUpdated) >= REBALANCE_NOJOBS_TIMEOUT {
						// do not send any more further jobs to this pod until
						server.Hasher[key].SendJob = false
					}
				}

				server.mu.Unlock()
			}
		}

	}()
}
