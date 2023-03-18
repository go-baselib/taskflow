package taskflow

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"go.temporal.io/sdk/workflow"

	"github.com/go-baselib/taskflow/entity"
)

type taskNode struct {
	entity.Task
	taskLock   sync.Mutex
	taskParams map[string]map[string]interface{} // key: taskID value: param
	neighbors  map[string]*taskNode              // key: taskID value: node
}

func (n *taskNode) execute(ctx workflow.Context) (map[string]interface{}, error) {
	var err = ctx.Err()
	if err != nil {
		return nil, err
	}

	var (
		reason map[string]interface{}
		future workflow.Future
	)

	if _, hit := activityMap[n.ActivityName]; !hit {
		return nil, fmt.Errorf("not found activity: %s", n.ActivityName)
	}

	var af = activityMap[n.ActivityName]

	log.Printf("task: %s taskParams: %+v", n.Task.ID, n.taskParams)
	future = workflow.ExecuteActivity(ctx, af, n.taskParams)
	if err = future.Get(ctx, &reason); err != nil {
		return nil, err
	}

	return reason, nil
}

type DAG struct {
	dag               map[string]*taskNode
	onceInDegrees     *sync.Once
	inDegreesSchedule map[string]int
	inDegreesRWLock   *sync.RWMutex
	entryChan         workflow.Channel
	execChan          workflow.Channel
	wg                *sync.WaitGroup
	tasks             map[string]struct{}

	timeout time.Duration
}

func Workflow(ctx workflow.Context, flow entity.Flow) error {
	var dag, err = NewDAG(ctx, flow)
	if err != nil {
		return err
	}

	return dag.Schedule(ctx)
}

func NewDAG(ctx workflow.Context, flow entity.Flow) (*DAG, error) {
	var (
		timeout = time.Duration(flow.TimeoutSec) * time.Second
		taskIDs = make(map[string]struct{}, len(flow.Tasks))
		nodes   = make(map[string]*taskNode)
	)

	for _, t := range flow.Tasks {
		if _, has := taskIDs[t.ID]; has {
			return nil, fmt.Errorf("task %s already exists", t.ID)
		}
		taskIDs[t.ID] = struct{}{}
	}

	for _, t := range flow.Tasks {
		if _, has := nodes[t.ID]; !has {
			nodes[t.ID] = &taskNode{
				Task:       t, // task
				taskParams: make(map[string]map[string]interface{}),
				neighbors:  make(map[string]*taskNode),
			}
			nodes[t.ID].taskParams[t.ID] = t.Params
		}

		// neighbors
		for _, taskID := range t.DependOn {
			if _, has := taskIDs[taskID]; !has {
				return nil, fmt.Errorf("task %s not found", taskID)
			}

			if _, has := nodes[taskID]; !has {
				nodes[taskID] = &taskNode{
					Task:       t, // task
					taskParams: make(map[string]map[string]interface{}),
					neighbors:  make(map[string]*taskNode),
				}
				nodes[taskID].taskParams[taskID] = t.Params
			}

			// 当前任务 t.ID() 依赖于 taskID 任务完成
			// 因此在任务 taskID 完成时通知 任务 t.ID()
			nodes[taskID].neighbors[t.ID] = nodes[t.ID]
		}
	}

	return &DAG{
		dag:               nodes,
		onceInDegrees:     &sync.Once{},
		inDegreesSchedule: make(map[string]int),
		inDegreesRWLock:   &sync.RWMutex{},
		entryChan:         workflow.NewChannel(ctx),
		execChan:          workflow.NewChannel(ctx),
		wg:                &sync.WaitGroup{},
		tasks:             taskIDs,

		timeout: timeout,
	}, nil
}

func (d *DAG) Schedule(ctx workflow.Context) error {
	defer func() {
		log.Println("Schedule Done")
		d.entryChan.Close()
		log.Println("close entryChan")
	}()

	if err := d.check(); err != nil {
		return err
	}

	var ao = workflow.ActivityOptions{
		StartToCloseTimeout: d.timeout,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var (
		en                      = d.entryNodes()
		selector                = workflow.NewSelector(ctx)
		childCtx, cancelHandler = workflow.WithCancel(ctx)
		activityErr             error
	)

	workflow.Go(ctx, func(ctx workflow.Context) {
		var tn taskNode
		for d.entryChan.Receive(ctx, &tn) {

			var ttn taskNode
			_ = copier.Copy(&ttn, &tn)

			log.Println("准备执行任务 ", ttn.Task.ID)

			workflow.Go(ctx, func(ctx workflow.Context) {
				d.execChan.Send(ctx, ttn.Task.ID)
			})

			var f = executeAsync(d, childCtx, &ttn)
			selector.AddFuture(f, func(f workflow.Future) {
				err := f.Get(ctx, nil)
				if err != nil {
					// cancel all pending activities
					cancelHandler()
					activityErr = err
				}
			})
		}

		d.execChan.Close()
		log.Println("close execChan")
	})

	for _, n := range en {
		d.entryChan.Send(ctx, n)
	}

	var (
		id  string
		cnt int
	)
	for d.execChan.Receive(ctx, &id) {
		selector.Select(ctx) // this will wait for one branch
		if activityErr != nil {
			log.Println(activityErr)
			return activityErr
		}

		if _, hit := d.tasks[id]; !hit {
			return fmt.Errorf("not found task: %s", id)
		}
		cnt++
		if cnt >= len(d.tasks) {
			return nil
		}
	}

	return nil
}

func executeAsync(d *DAG, ctx workflow.Context, n *taskNode) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		var err = d.execute(ctx, n)
		settable.Set(nil, err)
	})
	return future
}

func (d *DAG) execute(ctx workflow.Context, node *taskNode) error {
	if !d.isEntry(node.ID) {
		return nil
	}

	var reason, err = node.execute(ctx)
	if err != nil {
		return err
	}
	log.Println("处理成功: ", node.Task.ID)

	for _, e := range node.neighbors {
		// 入度 -1
		d.decrease(e)

		e.taskLock.Lock()
		if e.taskParams == nil {
			e.taskParams = make(map[string]map[string]interface{})
		}
		if reason != nil {
			e.taskParams[node.ID] = copyParams(reason)
		}
		e.taskLock.Unlock()

		log.Printf("任务 %s 入度 %d \n", e.Task.ID, d.inDegreesSchedule[e.ID])
		if d.isEntry(e.ID) {
			// 可执行
			log.Printf("任务 %s 提交执行 \n", e.Task.ID)
			d.entryChan.Send(ctx, e)
		}
	}

	return nil
}

func copyParams(p map[string]interface{}) map[string]interface{} {
	var params = make(map[string]interface{})
	_ = copier.Copy(&params, p)
	return params
}

func (d *DAG) decrease(node *taskNode) {
	d.inDegreesRWLock.Lock()
	defer d.inDegreesRWLock.Unlock()

	d.inDegreesSchedule[node.ID]--
}

func (d *DAG) inDegrees() map[string]int {
	d.onceInDegrees.Do(func() {
		var inDegrees = make(map[string]int)
		for _, n := range d.dag {
			for _, neighbor := range n.neighbors {
				// 被当前节点指向节点，入度 +1
				inDegrees[neighbor.ID]++
			}
		}
		d.inDegreesSchedule = inDegrees
	})

	d.inDegreesRWLock.RLock()
	defer d.inDegreesRWLock.RUnlock()

	var inDegrees = make(map[string]int)
	for name, inDegree := range d.inDegreesSchedule {
		inDegrees[name] = inDegree
	}
	return inDegrees
}

func (d *DAG) isEntry(name string) bool {
	d.inDegreesRWLock.RLock()
	defer d.inDegreesRWLock.RUnlock()

	return d.inDegreesSchedule[name] == 0
}

func (d *DAG) entryNodes() []*taskNode {
	var (
		inDegrees = d.inDegrees()
		queue     = make([]*taskNode, 0)
	)
	// 将入度为0的节点加入队列
	for _, n := range d.dag {
		if inDegrees[n.ID] == 0 {
			queue = append(queue, n)
		}
	}
	return queue
}

func (d *DAG) checkTask(name string) error {
	if _, has := d.tasks[name]; !has {
		return fmt.Errorf("task %s not found", name)
	}

	return nil
}

func (d *DAG) check() error {
	var (
		inDegrees = d.inDegrees()
		queue     = d.entryNodes()
		err       error
	)

	for len(queue) > 0 {
		var n = queue[0]
		if err = d.checkTask(n.ID); err != nil {
			return err
		}

		queue = queue[1:]

		for _, neighbor := range n.neighbors {
			if err = d.checkTask(neighbor.ID); err != nil {
				return err
			}

			inDegrees[neighbor.ID]--

			if inDegrees[neighbor.ID] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	for _, degree := range inDegrees {
		if degree > 0 {
			return fmt.Errorf("has cycle")
		}
	}

	return nil
}

type ActivityFunc func(ctx context.Context, taskParams map[string]map[string]interface{}) (map[string]interface{},
	error)

var activityMap = make(map[string]ActivityFunc)

func RegisterActivityFunc(activityName string, activity ActivityFunc) {
	activityMap[activityName] = activity
}
