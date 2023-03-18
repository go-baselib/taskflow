package main

import (
	"context"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/go-baselib/taskflow"
)

func main() {
	log.Println("starting")
	c, err := client.Dial(client.Options{
		HostPort: "192.168.8.42:7233",
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()
	log.Println("Client created")

	taskflow.RegisterActivityFunc("echo1", Echo1)
	taskflow.RegisterActivityFunc("echo2", Echo2)
	taskflow.RegisterActivityFunc("echo3", Echo3)
	taskflow.RegisterActivityFunc("echo4", Echo4)

	w := worker.New(c, "test-DAG", worker.Options{})
	w.RegisterWorkflow(taskflow.Workflow)
	w.RegisterActivity(Echo1)
	w.RegisterActivity(Echo2)
	w.RegisterActivity(Echo3)
	w.RegisterActivity(Echo4)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}

func Echo1(ctx context.Context, taskParams map[string]map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(time.Second)
	log.Printf("taskParams: %v \n", taskParams)
	return map[string]interface{}{
		"echo": "1",
	}, nil
}

func Echo2(ctx context.Context, taskParams map[string]map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(time.Second)

	log.Printf("taskParams: %v \n", taskParams)
	return map[string]interface{}{
		"echo": "2",
	}, nil
}

func Echo3(ctx context.Context, taskParams map[string]map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(time.Second)

	log.Printf("taskParams: %v \n", taskParams)
	return map[string]interface{}{
		"echo": "3",
	}, nil
}

func Echo4(ctx context.Context, taskParams map[string]map[string]interface{}) (map[string]interface{}, error) {
	time.Sleep(time.Second)

	log.Printf("taskParams: %v \n", taskParams)
	return map[string]interface{}{
		"echo": "4",
	}, nil
}
