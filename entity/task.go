package entity

type Task struct {
	ID           string
	ActivityName string
	DependOn     []string
	Params       map[string]interface{}
}
