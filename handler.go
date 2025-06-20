package main

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetAll,
	"COMMAND": command,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Wrong number of arguments for SET command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Wrong number of arguments for GET command"}
	}

	key := args[0].bulk

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

func hset(args []Value) Value {

	if len(args) != 3 {
		return Value{typ: "error", str: "Wrong number of arguments for HSET command"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMutex.Lock()
	defer HSETsMutex.Unlock()

	_, ok := HSETs[hash]
	if !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "Wrong number of arguments for HGET command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	m, ok := HSETs[hash]
	if !ok {
		return Value{typ: "null"}
	}

	value, ok := m[key]
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetAll(args []Value) Value {

	if len(args) != 1 {
		return Value{typ: "error", str: "Wrong number of arguments for HGETALL command"}
	}

	hash := args[0].bulk

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	val, ok := HSETs[hash]
	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	var value []Value
	for x, y := range val {
		value = append(value, Value{typ: "bulk", bulk: x})
		value = append(value, Value{typ: "bulk", bulk: y})
	}

	return Value{typ: "array", array: value}
}

func command(args []Value) Value {
	return Value{typ: "array", array: []Value{}}
}
