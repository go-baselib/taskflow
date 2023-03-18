package main

import (
	"context"
	"log"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/go-baselib/taskflow"
	"github.com/go-baselib/taskflow/entity"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort: "192.168.8.42:7233",
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	workflowOptions := client.StartWorkflowOptions{
		ID:        uuid.NewString(),
		TaskQueue: "test-DAG",
	}

	var flow entity.Flow
	flow.ID = "test-flow"
	flow.TimeoutSec = 100
	flow.Tasks = []entity.Task{
		{ID: "echo1", ActivityName: "echo1", DependOn: nil, Params: map[string]interface{}{"task": "echo1"}},
		{ID: "echo2", ActivityName: "echo2", DependOn: []string{"echo1"},
			Params: map[string]interface{}{"task": "echo2"}},
		{ID: "echo3", ActivityName: "echo3", DependOn: []string{"echo1", "echo2"},
			Params: map[string]interface{}{"task": "echo3"}},
		{ID: "echo4", ActivityName: "echo4", DependOn: []string{"echo2"},
			Params: map[string]interface{}{"task": "echo4"}},
		{ID: "echo5", ActivityName: "echo1", DependOn: []string{"echo4", "echo1"},
			Params: map[string]interface{}{"task": "echo5"}},
		{ID: "echo6", ActivityName: "echo2", DependOn: []string{}, Params: map[string]interface{}{"task": "echo6"}},
		{ID: "echo7", ActivityName: "echo3", DependOn: []string{"echo6"},
			Params: map[string]interface{}{"task": "echo7"}},
		{ID: "echo8", ActivityName: "echo4", DependOn: []string{"echo7"},
			Params: map[string]interface{}{"task": "echo8"}},
		{ID: "echo9", ActivityName: "echo1", DependOn: []string{"echo6"},
			Params: map[string]interface{}{"task": "echo9"}},
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, taskflow.Workflow, flow)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	if err = we.Get(context.Background(), nil); err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow done")
}
