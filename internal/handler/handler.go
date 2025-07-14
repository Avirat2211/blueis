package handler

import (
	"strconv"
	"sync"
	"time"

	"github.com/Avirat2211/blueis/internal/resp"
)

var Handlers = map[string]func([]resp.Value) resp.Value{
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

func ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}
	return resp.Value{Typ: "string", Str: args[0].Bulk}
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

func set(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for SET command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

func get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for GET command"}
	}

	key := args[0].Bulk

	cleanupIfExpired(key)

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "Bulk", Bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

func hset(args []resp.Value) resp.Value {

	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HSET command"}
	}
	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	HSETsMutex.Lock()
	defer HSETsMutex.Unlock()

	_, ok := HSETs[hash]
	if !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value

	return resp.Value{Typ: "string", Str: "OK"}
}

func hget(args []resp.Value) resp.Value {

	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HGET command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	cleanupIfExpired(hash)

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	m, ok := HSETs[hash]
	if !ok {
		return resp.Value{Typ: "null"}
	}

	value, ok := m[key]
	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "Bulk", Bulk: value}
}

func hgetAll(args []resp.Value) resp.Value {

	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for HGETALL command"}
	}

	hash := args[0].Bulk

	cleanupIfExpired(hash)

	HSETsMutex.RLock()
	defer HSETsMutex.RUnlock()

	val, ok := HSETs[hash]
	if !ok {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}

	var value []resp.Value
	for x, y := range val {
		value = append(value, resp.Value{Typ: "Bulk", Bulk: x})
		value = append(value, resp.Value{Typ: "Bulk", Bulk: y})
	}

	return resp.Value{Typ: "array", Array: value}
}

func command(args []resp.Value) resp.Value {
	return resp.Value{Typ: "Array", Array: []resp.Value{}}
}

var Expiry = map[string]int64{}
var ExpiryMutex = sync.RWMutex{}

func expire(args []resp.Value) resp.Value {

	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for EXPIRE command"}
	}

	key := args[0].Bulk

	seconds, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Value{Typ: "error", Str: "Invalid seconds value for EXPIRE command"}
	}

	SETsMutex.RLock()
	_, setOk := SETs[key]
	SETsMutex.RUnlock()

	HSETsMutex.RLock()
	_, hsetOk := HSETs[key]
	HSETsMutex.RUnlock()

	if !setOk && !hsetOk {
		return resp.Value{Typ: "string", Str: "0"}
	}

	expiryTime := time.Now().Unix() + int64(seconds)

	ExpiryMutex.Lock()
	Expiry[key] = expiryTime
	ExpiryMutex.Unlock()

	return resp.Value{Typ: "string", Str: "1"}

}

func ttl(args []resp.Value) resp.Value {

	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "Wrong number of arguments for TTL command"}
	}

	key := args[0].Bulk

	ExpiryMutex.RLock()
	exp, ok := Expiry[key]
	ExpiryMutex.RUnlock()

	if !ok {
		return resp.Value{Typ: "string", Str: "-1"}
	}

	ttl := exp - time.Now().Unix()

	if ttl < 0 {
		return resp.Value{Typ: "string", Str: "-2"}
	}

	return resp.Value{Typ: "string", Str: strconv.Itoa(int(ttl))}
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
