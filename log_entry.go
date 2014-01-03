package raft

import (
	"bufio"
	"bytes"
	"errors"
	"hash/crc32"
	"fmt"
	"io"
	"encoding/json"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A log entry stores a single item in the log.
type LogEntry struct {
	log     *Log
	index   uint64
	term    uint64
	command Command
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log entry associated with a log.
func NewLogEntry(log *Log, index uint64, term uint64, command Command) *LogEntry {
	return &LogEntry{
		log: log,
		index: index,
		term: term,
		command: command,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Encoding
//--------------------------------------

// Encodes the log entry to a buffer.
// 持久化log entry
func (e *LogEntry) Encode(w io.Writer) error {
	if w == nil {
		return errors.New("raft.LogEntry: Writer required to encode")
	}

    // 将Command对象encode为json字符串
	encodedCommand, err := json.Marshal(e.command)
	if err != nil {
		return err
	}

	// Write log line to temporary buffer.
    // 将log entry的基本信息写入buffer
    // 其中第三列单独把command name列出来，是因为Command是一个接口类
    // 实际使用的时候，客户端发来的command都是实现Command借口的具体的类的对象
    // 以后decode的时候，要根据command name来new出对应的command
	var b bytes.Buffer
	if _, err = fmt.Fprintf(&b, "%016x %016x %s %s\n", e.index, e.term, e.command.Name(), encodedCommand); err != nil {
		return err
	}

	// Generate checksum.
	checksum := crc32.ChecksumIEEE(b.Bytes())

	// Write log entry with checksum.
	_, err = fmt.Fprintf(w, "%08x %s", checksum, b.String())
	return err
}

// Decodes the log entry from a buffer. Returns the number of bytes read.
// 从log中把log entry恢复出来，并返回本次读了多少byte
func (e *LogEntry) Decode(r io.Reader) (pos int, err error) {
	pos = 0

	if r == nil {
		err = errors.New("raft.LogEntry: Reader required to decode")
		return
	}

	// Read the expected checksum first.
	var checksum uint32
	if _, err = fmt.Fscanf(r, "%08x", &checksum); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to read checksum: %v", err)
		return
	}
	pos += 8

	// Read the rest of the line.
	bufr := bufio.NewReader(r)
	if c, _ := bufr.ReadByte(); c != ' ' {
		err = fmt.Errorf("raft.LogEntry: Expected space, received %02x", c)
		return
	}
	pos += 1

	line, err := bufr.ReadString('\n')
	pos += len(line)
	if err == io.EOF {
		err = fmt.Errorf("raft.LogEntry: Unexpected EOF")
		return
	} else if err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to read line: %v", err)
		return
	}
    // BufferString，方便scanf
	b := bytes.NewBufferString(line)

	// Verify checksum.
	bchecksum := crc32.ChecksumIEEE(b.Bytes())
	if checksum != bchecksum {
		err = fmt.Errorf("raft.LogEntry: Invalid checksum: Expected %08x, calculated %08x", checksum, bchecksum)
		return
	}

	// Read term, index and command name.
	var commandName string
	if _, err = fmt.Fscanf(b, "%016x %016x %s ", &e.index, &e.term, &commandName); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to scan: %v", err)
		return
	}

	// Instantiate command by name.
	command, err := e.log.NewCommand(commandName)
	if err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to instantiate command (%s): %v", commandName, err)
		return
	}

	// Deserialize command.
    // 直接从BufferString中decode出command对象
	if err = json.NewDecoder(b).Decode(&command); err != nil {
		err = fmt.Errorf("raft.LogEntry: Unable to decode: %v", err)
		return
	}
	e.command = command

	// Make sure there's only an EOF remaining.
	c, err := b.ReadByte()
	if err != io.EOF {
		err = fmt.Errorf("raft.LogEntry: Expected EOL, received %02x", c)
		return
	}

	err = nil
	return
}
