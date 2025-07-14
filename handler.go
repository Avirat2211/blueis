package main

import (
	"strconv"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetAll,
	"COMMAND": command,
	"EXPIRE":  expire,
	"TTL":     ttl,
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

	cleanupIfExpired(key)

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

	cleanupIfExpired(hash)

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

	cleanupIfExpired(hash)

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

var Expiry = map[string]int64{}
var ExpiryMutex = sync.RWMutex{}

func expire(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "Wrong number of arguments for EXPIRE command"}
	}

	key := args[0].bulk

	seconds, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "Invalid seconds value for EXPIRE command"}
	}

	SETsMutex.RLock()
	_, setOk := SETs[key]
	SETsMutex.RUnlock()

	HSETsMutex.RLock()
	_, hsetOk := HSETs[key]
	HSETsMutex.RUnlock()

	if !setOk && !hsetOk {
		return Value{typ: "string", str: "0"}
	}

	expiryTime := time.Now().Unix() + int64(seconds)

	ExpiryMutex.Lock()
	Expiry[key] = expiryTime
	ExpiryMutex.Unlock()

	return Value{typ: "string", str: "1"}

}

func ttl(args []Value) Value {

	if len(args) != 1 {
		return Value{typ: "error", str: "Wrong number of arguments for TTL command"}
	}

	key := args[0].bulk

	ExpiryMutex.RLock()
	exp, ok := Expiry[key]
	ExpiryMutex.RUnlock()

	if !ok {
		return Value{typ: "string", str: "-1"}
	}

	ttl := exp - time.Now().Unix()

	if ttl < 0 {
		return Value{typ: "string", str: "-2"}
	}

	return Value{typ: "string", str: strconv.Itoa(int(ttl))}
}

func isExpired(key string) bool {

	ExpiryMutex.RLock()
	exp, ok := Expiry[key]
	ExpiryMutex.RUnlock()

	if !ok {
		return false
	}

	return time.Now().Unix() > exp
}

func cleanupIfExpired(key string) {

	if isExpired(key) {

		SETsMutex.Lock()
		delete(SETs, key)
		SETsMutex.Unlock()

		HSETsMutex.Lock()
		delete(HSETs, key)
		HSETsMutex.Unlock()

		ExpiryMutex.Lock()
		delete(Expiry, key)
		ExpiryMutex.Unlock()
	}
}
