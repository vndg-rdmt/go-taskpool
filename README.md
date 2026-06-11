# Lockfree in-memory task queue and worker pool for Golang

Package provides tools for creating event-driven apps with blazing
task processing. It does not provides a persistent event storage or a
message/event broker, it provides a tool to handle tasks proccesing in-memory.

## What cases this lib handles

> Persistent tasks handling

For example you need to send a batch of messages from your app
and thoose messages must be delivered even it app restarted or etc.

You create a your specific case task storage, for example simply store
thoose tasks to send message in db. Then you pick tasks up in job every
period of time and load into memory.

To not load them one-by-one, you can load them into memory `taskarena.TaskPool`
as a batch and `taskarena.WorkerPool` will handle them up as fast as the task
is handled within your defined task handler.

> Worker pool

Do not reimplemenet worker pool each time, just use it. You can place
any of worker pool use cases here and all of them will be valid to use
this library.

> Generic

Prevent Golang `interface{}` 'features' like allocation. Need to implement
specific case for task entity? Use structure type explicitly. Need to implement
type agnostic task handling like in event brokers? Define something that holds
up `task-id`/`subject` and data as `[]byte`

## Installation

```sh
go get github.com/vndg-rdmt/go-taskarena
```