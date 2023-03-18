package entity

type Flow struct {
	ID         string
	Tasks      []Task
	TimeoutSec int
}
