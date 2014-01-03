package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A log is a collection of log entries that are persisted to durable storage.
type Log struct {
	file *os.File
	entries      []*LogEntry
	commitIndex  uint64
	commandTypes map[string]Command
	mutex sync.Mutex
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log.
func NewLog() *Log {
	return &Log{
		commandTypes: make(map[string]Command),
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Commands
//--------------------------------------

// Instantiates a new command by type name. Returns an error if the command type
// has not been registered already.
// 根据command name来反射出具体的command class，然后new出相应的对象
func (l *Log) NewCommand(name string) (Command, error) {
	// Find the registered command.
	command := l.commandTypes[name]
	if command == nil {
		return nil, fmt.Errorf("raft.Log: Unregistered command type: %s", name)
	}

	// Make a copy of the command.
	v := reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy, ok := v.(Command)
	if !ok {
		panic(fmt.Sprintf("raft.Log: Unable to copy command: %s (%v)", command.Name(), reflect.ValueOf(v).Kind().String()))
	}
	return copy, nil
}

// Adds a command type to the log. The instance passed in will be copied and
// deserialized each time a new log entry is read. This function will panic
// if a command type with the same name already exists.
func (l *Log) AddCommandType(command Command) {
	if command == nil {
		panic(fmt.Sprintf("raft.Log: Command type cannot be nil"))
	} else if l.commandTypes[command.Name()] != nil {
		panic(fmt.Sprintf("raft.Log: Command type already exists: %s", command.Name()))
	}
	l.commandTypes[command.Name()] = command
}

//--------------------------------------
// State
//--------------------------------------

// Opens the log file and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
// Open做了两件事
// 1. 读出log文件里所有的log entry
// 2. 打开log文件，供追加log entry
func (l *Log) Open(path string) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Read all the entries from the log if one exists.
	var lastIndex int = 0
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		// Open the log file.
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader := bufio.NewReader(file)

		// Read the file and decode entries.
		for {
			if _, err := reader.Peek(1); err == io.EOF {
				break
			}

			// Instantiate log entry and decode into it.
			entry := NewLogEntry(l, 0, 0, nil)
			n, err := entry.Decode(reader)
			if err != nil {
				warn("raft.Log: %v", err)
				warn("raft.Log: Recovering (%d)", lastIndex)
				file.Close()
				if err = os.Truncate(path, int64(lastIndex)); err != nil {
					return fmt.Errorf("raft.Log: Unable to recover: %v", err)
				}
				break
			}
			l.commitIndex = entry.index
			lastIndex += n

			// Append entry.
			l.entries = append(l.entries, entry)
		}

		file.Close()
	}

	// Open the file for appending.
	var err error
	l.file, err = os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	return nil
}

// Closes the log file.
func (l *Log) Close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	l.entries = make([]*LogEntry, 0)
}

//--------------------------------------
// Append
//--------------------------------------

// Updates the commit index and writes entries after that index to the stable
// storage.
func (l *Log) SetCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Do not allow previous indices to be committed again.
	if index < l.commitIndex {
		return fmt.Errorf("raft.Log: Commit index (%d) ahead of requested commit index (%d)", l.commitIndex, index)
	}

	// Find all entries whose index is between the previous index and the current index.
	for _, entry := range l.entries {
		if entry.index > l.commitIndex && entry.index <= index {
			// Write to storage.
			if err := entry.Encode(l.file); err != nil {
				return err
			}

			// Update commit index.
			l.commitIndex = entry.index
		}
	}

	return nil
}

//--------------------------------------
// Append
//--------------------------------------

// Writes a single log entry to the end of the log.
func (l *Log) Append(entry *LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file == nil {
		return errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.term < lastEntry.term {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x < %x:%x)", entry.term, entry.index, lastEntry.term, lastEntry.index)
		} else if entry.index == lastEntry.index && entry.index <= lastEntry.index {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x < %x:%x)", entry.term, entry.index, lastEntry.term, lastEntry.index)
		}
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return nil
}
