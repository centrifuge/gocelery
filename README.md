# gocelery

Task runner in Go

## Why?
[Go Centrifuge Node](https://github.com/centrifuge/go-centrifuge) needs a task runner with first class support to run jobs with stateful runners.

## Storage
Storage is used to store the jobs and other queuing related details
Currently supported storage: LevelDb, InMemory

## Queue
A Queue would enqueue jobs and dequeues when ready to be executed. A default queue implementation is provided.

## Dispatcher
A dispatcher uses Storage and Queue to run tasks asynchronously.

## Job
A Job defines a single job. it can encompasses multiple tasks and has a validity time. if fails, backoffs exponentially
until either successful or expired. 
Dispatcher uses gob encoder to encode the job params like arguments, overrides etc... Register your custom types with gob before dispatching a job.

## Examples
Checkout `examples` package for more examples

## Contributing
You are more than welcome to make any contributions.
Please create Pull Request for any changes.

## LICENSE

The gocelery is offered under MIT license.
